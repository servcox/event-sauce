using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Serialization;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce;

public class EventStore(String topic, BlobContainerClient client) : EventStore<Dictionary<String, String>>(topic, client);

public class EventStore<TMetadata>(String topic, BlobContainerClient client)
{
    private readonly EventTypeResolver _eventTypeResolver = new();
    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private readonly Byte[] _fieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private readonly Byte[] _recordSeparatorBytes = { Convert.ToByte(RecordSeparator) };
    private UInt64 _slice;
    private const String DateFormatString = @"yyyyMMdd\THHmmss\Z";

    private readonly JsonSerializerOptions _serializationOptions = new()
    {
#if NET5_0_OR_GREATER
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
#else
        IgnoreNullValues = true,
#endif
    };

    public Task Write(Object payload, TMetadata? metadata = default, CancellationToken cancellationToken = default) =>
        Write(new Event<TMetadata> { Payload = payload, Metadata = metadata }, cancellationToken);

    public Task Write(IEvent<TMetadata> evt, CancellationToken cancellationToken = default) =>
        Write(new List<IEvent<TMetadata>> { evt }, cancellationToken);

    public async Task Write(IEnumerable<IEvent<TMetadata>> events, CancellationToken cancellationToken = default)
    {
        if (events is null) throw new ArgumentNullException(nameof(events));

        using var stream = EncodeEventsAsStream(events);

        while (true)
        {
            var slice = _slice;
            var blob = await GetCreateSliceBlob(slice, cancellationToken).ConfigureAwait(false);

            if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
                throw new TransactionTooLargeException($"When encoded transaction is {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes");

            try
            {
                await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "BlockCountExceedsLimit")
            {
                Trace.WriteLine($"Topic {topic} reached max writes on slice {slice}");
                _slice = slice + 1;
            }
        }
    }


    public async Task<IEnumerable<IEgressEvent<TMetadata>>> Read(UInt64 startSlice = 0, UInt64 startSliceOffset = 0, UInt32 maxEvents = UInt32.MaxValue)
    {
        var output = new List<IEgressEvent<TMetadata>>();

        for (var slice = startSlice; slice < UInt64.MaxValue; slice++)
        {
            var sliceBlob = GetSliceBlob(slice);
            var scopedOffset = (Int64)(slice == startSlice ? startSliceOffset : 0);

            try
            {
                using var stream = await sliceBlob.OpenReadAsync(scopedOffset).ConfigureAwait(false);
                var subResults = DecodeEventsFromStream(stream, slice, maxEvents - (UInt32)output.Count);
                output.AddRange(subResults);
                if (output.Count >= maxEvents) return output;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "BlobNotFound")
            {
                return output;
            }
        }

        return output;
    }

    private AppendBlobClient GetSliceBlob(UInt64 slice) => client.GetAppendBlobClient($"{topic.ToUpperInvariant()}.{slice.ToPaddedString()}.tsv");

    private async Task<AppendBlobClient> GetCreateSliceBlob(UInt64 slice, CancellationToken cancellationToken)
    {
        var blob = GetSliceBlob(slice);
        await blob.CreateIfNotExistsAsync(httpHeaders: new()
        {
            ContentType = "text/tab-separated-values",
        }, cancellationToken: cancellationToken).ConfigureAwait(false);
        return blob;
    }

    private MemoryStream EncodeEventsAsStream(IEnumerable<IEvent<TMetadata>> events)
    {
        var at = DateTime.UtcNow.ToString(DateFormatString, CultureInfo.InvariantCulture);
        var stream = new MemoryStream();
        foreach (var evt in events)
        {
            var typeName = _eventTypeResolver.Encode(evt.Payload.GetType());
            stream.WriteAsUtf8(typeName);
            stream.Write(_fieldSeparatorBytes);

            stream.WriteAsUtf8(at);
            stream.Write(_fieldSeparatorBytes);

            if (evt.Payload is null) throw new NullPayloadException("One or more provided payloads are null");
            var payloadEncoded = JsonSerializer.Serialize(evt.Payload, _serializationOptions);
            stream.WriteAsUtf8(payloadEncoded);
            stream.Write(_fieldSeparatorBytes);

            var metadataEncoded = JsonSerializer.Serialize(evt.Metadata, _serializationOptions);
            stream.WriteAsUtf8(metadataEncoded);
            stream.Write(_recordSeparatorBytes);
        }

        stream.Rewind();
        return stream;
    }

    private List<IEgressEvent<TMetadata>> DecodeEventsFromStream(Stream stream, UInt64 slice, UInt32 maxCount)
    {
        using var reader = new StreamReader(stream, false);
        var output = new List<IEgressEvent<TMetadata>>();
        while (!reader.EndOfStream)
        {
            var startOffset = (UInt64)reader.BaseStream.Position;
            var line = reader.ReadLine()!;
            var endOffset = (UInt64)reader.BaseStream.Position;
            var tokens = line.Split(FieldSeparator);
            var typeName = tokens[0];
            var type = _eventTypeResolver.TryDecode(typeName) ?? typeof(Object);
            var at = DateTime.ParseExact(tokens[1], DateFormatString, CultureInfo.InvariantCulture);
            var payload = JsonSerializer.Deserialize(tokens[2], type, _serializationOptions) ?? throw new NeverNullException();
            var metadata = JsonSerializer.Deserialize<TMetadata>(tokens[3], _serializationOptions);

            output.Add(new EgressEvent<TMetadata>
            {
                Type = typeName,
                Payload = payload,
                Metadata = metadata,
                At = at,
                Slice = slice,
                StartOffset = startOffset,
                EndOffset = endOffset,
            });

            if (--maxCount == 0) return output;
        }

        return output;
    }
}