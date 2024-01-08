using System.Globalization;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

// ReSharper disable UseAwaitUsing
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce;

public class EventStore(String topic, BlobContainerClient client)
    : EventStore<Dictionary<String, String>>(topic, client);

public class EventStore<TMetadata>(String topic, BlobContainerClient client)
{
    private readonly EventTypeResolver _eventTypeResolver = new();
    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private readonly Byte[] _fieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private readonly Byte[] _recordSeparatorBytes = { Convert.ToByte(RecordSeparator) };
    private UInt64 _lastKnownCurrentSlice;
    private const String DateFormatString = @"yyyyMMdd\THHmmss\Z";
    private const Int32 TargetBlocksPerSlice = 1000;

    private readonly BlobHttpHeaders _sliceBlobHeaders = new()
    {
        ContentType = "text/tab-separated-values",
    };

    private readonly JsonSerializerOptions _serializationOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
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
            var blob = await GetCurrentSliceBlob(cancellationToken).ConfigureAwait(false);

            if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
                throw new TransactionTooLargeException($"When encoded transaction is {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes");

            await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<IEnumerable<IEgressEvent<TMetadata>>> Read(UInt64 startSlice = 0, UInt64 startSliceOffset = 0, UInt32 maxEvents = UInt32.MaxValue, CancellationToken cancellationToken = default)
    {
        var output = new List<IEgressEvent<TMetadata>>();

        for (var slice = startSlice; slice < UInt64.MaxValue; slice++)
        {
            var sliceBlob = GetSliceBlob(slice);
            var scopedOffset = (Int64)(slice == startSlice ? startSliceOffset : 0);

            if (!await sliceBlob.ExistsAsync(cancellationToken).ConfigureAwait(false)) return output;

            using var stream = await sliceBlob.OpenReadAsync(scopedOffset, cancellationToken: cancellationToken).ConfigureAwait(false);

            output.AddRange(DecodeEventsFromStream(stream, slice, maxEvents - (UInt32)output.Count));
            if (output.Count >= maxEvents) return output;
        }

        return output;
    }

    private AppendBlobClient GetSliceBlob(UInt64 slice) =>
        client.GetAppendBlobClient($"{topic.ToUpperInvariant()}.{slice.ToPaddedString()}.tsv");

    private async Task<AppendBlobClient> GetCurrentSliceBlob(CancellationToken cancellationToken)
    {
        for (var slice = _lastKnownCurrentSlice; slice < UInt64.MaxValue; slice++)
        {
            var blob = GetSliceBlob(slice);

            await blob.CreateIfNotExistsAsync(httpHeaders: _sliceBlobHeaders, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            _lastKnownCurrentSlice = slice;

            var properties = await blob.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            if (properties.Value.BlobCommittedBlockCount < TargetBlocksPerSlice) return blob;
        }

        throw new NeverException();
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
            var offset = (UInt64)reader.BaseStream.Position;
            var line = reader.ReadLine()!;
            if (line.Length == 0) continue; // Skip blank lines - used in testing
            var nextOffset = (UInt64)reader.BaseStream.Position;
            var tokens = line.Split(FieldSeparator);
            var typeName = tokens[0];
            var type = _eventTypeResolver.TryDecode(typeName) ?? typeof(Object);
            var at = DateTime.ParseExact(tokens[1], DateFormatString, CultureInfo.InvariantCulture);
            var payload = JsonSerializer.Deserialize(tokens[2], type, _serializationOptions) ??
                          throw new NeverException();
            var metadata = JsonSerializer.Deserialize<TMetadata>(tokens[3], _serializationOptions);

            output.Add(new EgressEvent<TMetadata>
            {
                Type = typeName,
                Payload = payload,
                Metadata = metadata,
                At = at,
                Slice = slice,
                Offset = offset,
                NextOffset = nextOffset,
            });

            if (--maxCount == 0) return output;
        }

        return output;
    }
}