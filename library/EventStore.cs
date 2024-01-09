using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using ServcoX.EventSauce.ConfigurationBuilders;
using Stream = System.IO.Stream;

// ReSharper disable UseAwaitUsing
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce;

public class EventStore
{
    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly EventStoreConfiguration _configuration = new();
    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private readonly Byte[] _fieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private readonly Byte[] _recordSeparatorBytes = { Convert.ToByte(RecordSeparator) };
    private UInt64 _lastKnownCurrentSlice;
    private const String DateFormatString = @"yyyyMMdd\THHmmss\Z";
    public BlobContainerClient UnderlyingContainerClient => _client;

    private readonly BlobHttpHeaders _sliceBlobHeaders = new()
    {
        ContentType = "text/tab-separated-values",
    };

    private readonly JsonSerializerOptions _serializationOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    private readonly String _aggregate;
    private readonly BlobContainerClient _client;

    private readonly Regex
        _aggregatePattern = new("^[A-Z0-9-_ ]{1,64}$"); // Tightened from https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#blob-names

    public EventStore(String aggregateName, BlobContainerClient client, Action<EventStoreConfiguration>? builder = null)
    {
        if (aggregateName is null || !_aggregatePattern.IsMatch(aggregateName)) throw new ArgumentException($"Must not be null and match pattern {_aggregatePattern}", nameof(aggregateName));
        _aggregate = aggregateName;
        _client = client;
        builder?.Invoke(_configuration);
    }

    public Task Write(String aggregateId, Object payload, IDictionary<String, String>? metadata = default, CancellationToken cancellationToken = default) =>
        Write(new Event { AggregateId = aggregateId, Payload = payload, Metadata = metadata ?? new Dictionary<String, String>() }, cancellationToken);

    public Task Write(IEvent evt, CancellationToken cancellationToken = default) =>
        Write(new List<IEvent> { evt }, cancellationToken);

    public Task Write(IEnumerable<Event> events, CancellationToken cancellationToken = default) =>
        Write(events.Cast<IEvent>(), cancellationToken);

    public async Task Write(IEnumerable<IEvent> events, CancellationToken cancellationToken = default)
    {
        if (events is null) throw new ArgumentNullException(nameof(events));

        using var stream = EncodeEventsAsStream(events);
        var blob = await GetCurrentSliceBlob(cancellationToken).ConfigureAwait(false);
        if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
            throw new TransactionTooLargeException($"Encoded events is is {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes. Write less or smaller events.");

        await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<IEgressEvent>> Read(UInt64 startSlice = 0, UInt64 startSliceOffset = 0, UInt32 maxEvents = UInt32.MaxValue, CancellationToken cancellationToken = default)
    {
        var output = new List<IEgressEvent>();

        for (var slice = startSlice; slice < UInt64.MaxValue; slice++)
        {
            var sliceBlob = GetSliceBlob(slice);
            var scopedOffset = (Int64)(slice == startSlice ? startSliceOffset : 0);

            if (!await sliceBlob.ExistsAsync(cancellationToken).ConfigureAwait(false)) return output;

            using var stream = await sliceBlob.OpenReadAsync(scopedOffset, cancellationToken: cancellationToken).ConfigureAwait(false);

            output.AddRange(DecodeEventsFromStream(stream, slice, maxEvents - (UInt32)output.Count, (UInt64)scopedOffset));
            if (output.Count >= maxEvents) return output;
        }

        return output;
    }

    private AppendBlobClient GetSliceBlob(UInt64 slice) =>
        _client.GetAppendBlobClient($"{_aggregate.ToUpperInvariant()}.{slice.ToPaddedString()}.tsv");

    private async Task<AppendBlobClient> GetCurrentSliceBlob(CancellationToken cancellationToken)
    {
        for (var slice = _lastKnownCurrentSlice; slice < UInt64.MaxValue; slice++)
        {
            var blob = GetSliceBlob(slice);

            await blob.CreateIfNotExistsAsync(httpHeaders: _sliceBlobHeaders, cancellationToken: cancellationToken).ConfigureAwait(false);
            _lastKnownCurrentSlice = slice;

            var properties = await blob.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            if (properties.Value.BlobCommittedBlockCount < _configuration.TargetBlocksPerSlice) return blob;
        }

        throw new NeverException();
    }

    private MemoryStream EncodeEventsAsStream(IEnumerable<IEvent> events)
    {
        var at = DateTime.UtcNow.ToString(DateFormatString, CultureInfo.InvariantCulture);
        var stream = new MemoryStream();
        foreach (var evt in events)
        {
            if (evt.AggregateId.Contains(FieldSeparator) || evt.AggregateId.Contains(RecordSeparator)) throw new BadEventException($"{nameof(Event.AggregateId)} cannot contain \t or \n");
            stream.WriteAsUtf8(evt.AggregateId);
            stream.Write(_fieldSeparatorBytes);

            var typeName = _eventTypeResolver.Encode(evt.Payload.GetType());
            stream.WriteAsUtf8(typeName);
            stream.Write(_fieldSeparatorBytes);

            stream.WriteAsUtf8(at);
            stream.Write(_fieldSeparatorBytes);

            if (evt.Payload is null) throw new BadEventException("One or more provided payloads are null");
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

    private List<IEgressEvent> DecodeEventsFromStream(Stream stream, UInt64 slice, UInt32 maxCount, UInt64 baseOffset)
    {
        var reader = new StreamLineReader(stream);
        var output = new List<IEgressEvent>();

        UInt64 lastPosition = 0;
        while (reader.TryReadLine() is { } line)
        {
            var offset = lastPosition + baseOffset;
            lastPosition = reader.Position;
            var nextOffset = lastPosition + baseOffset;

            if (line.Length == 0) continue; // Skip blank lines - used in testing
            var tokens = line.Split(FieldSeparator);
            var aggregateId = tokens[0];
            var typeName = tokens[1];
            var type = _eventTypeResolver.TryDecode(typeName) ?? typeof(Object);
            var at = DateTime.ParseExact(tokens[2], DateFormatString, CultureInfo.InvariantCulture);
            var payload = JsonSerializer.Deserialize(tokens[3], type, _serializationOptions) ?? throw new NeverException();
            var metadata = JsonSerializer.Deserialize<Dictionary<String, String>>(tokens[4], _serializationOptions) ?? new Dictionary<String, String>();

            output.Add(new EgressEvent
            {
                AggregateId = aggregateId,
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