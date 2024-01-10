using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;
using Stream = System.IO.Stream;

// ReSharper disable UseAwaitUsing
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce;

public class EventStore
{
    private const String DateFormatString = @"yyyyMMdd\THHmmss\Z";

    private static readonly BlobHttpHeaders SliceBlobHeaders = new()
    {
        ContentType = "text/tab-separated-values",
    };

    private static readonly JsonSerializerOptions SerializationOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    private static readonly Regex AggregateNamePattern = new("^[A-Z0-9-_ ]{1,64}$");

    private const Char FieldSeparator = '\t';
    private const Char RecordSeparator = '\n';
    private static readonly Byte[] FieldSeparatorBytes = { Convert.ToByte(FieldSeparator) };
    private static readonly Byte[] RecordSeparatorBytes = { Convert.ToByte(RecordSeparator) };

    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly EventStoreConfiguration _configuration = new();
    private readonly String _sliceBlobPathPrefix;
    private readonly String _sliceBlobPathPostfix = ".tsv";
    public String AggregateName { get; }
    private readonly BlobContainerClient _client;
    public BlobContainerClient UnderlyingContainerClient => _client;
    private Int64 _currentSliceId;

    public EventStore(String aggregateName, BlobContainerClient client, Action<EventStoreConfiguration>? builder = null)
    {
        if (aggregateName is null || !AggregateNamePattern.IsMatch(aggregateName)) throw new ArgumentException($"Must not be null and match pattern {AggregateNamePattern}", nameof(aggregateName));
        AggregateName = aggregateName;
        _sliceBlobPathPrefix = $"{aggregateName}/event/{aggregateName}.";
        _client = client;
        builder?.Invoke(_configuration);

        _currentSliceId = ListSlices().Result.LastOrDefault().Id;
    }

    public Task WriteEvent(String aggregateId, Object payload, IDictionary<String, String>? metadata = default, CancellationToken cancellationToken = default) =>
        WriteEvent(new Event { AggregateId = aggregateId, Payload = payload, Metadata = metadata ?? new Dictionary<String, String>() }, cancellationToken);

    public Task WriteEvent(IEvent evt, CancellationToken cancellationToken = default) =>
        WriteEvents(new List<IEvent> { evt }, cancellationToken);

    public Task WriteEvents(IEnumerable<Event> events, CancellationToken cancellationToken = default) =>
        WriteEvents(events.Cast<IEvent>(), cancellationToken);

    public async Task WriteEvents(IEnumerable<IEvent> events, CancellationToken cancellationToken = default)
    {
        if (events is null) throw new ArgumentNullException(nameof(events));

        using var stream = EncodeEventsAsStream(events);
        var blob = await GetCurrentSliceBlob(cancellationToken).ConfigureAwait(false);
        if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
            throw new TransactionTooLargeException($"Encoded events is is {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes. Write less or smaller events.");

        await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<Slice>> ListSlices()
    {
        var output = new List<Slice>();
        var sliceBlobs = _client.GetBlobsAsync(prefix: _sliceBlobPathPrefix);
        await foreach (var sliceBlob in sliceBlobs)
        {
            var idRaw = sliceBlob.Name.Substring(_sliceBlobPathPrefix.Length, sliceBlob.Name.Length - _sliceBlobPathPrefix.Length - _sliceBlobPathPostfix.Length);
            var id = Int64.Parse(idRaw, CultureInfo.InvariantCulture);
            var end = sliceBlob.Properties.ContentLength.HasValue ? sliceBlob.Properties.ContentLength.Value + 1 : 0;
            var createdAt = sliceBlob.Properties.CreatedOn ?? new DateTimeOffset();

            output.Add(new()
            {
                Id = id,
                End = end,
                CreatedAt = createdAt.UtcDateTime
            });
        }

        return output
            .OrderBy(a => a.CreatedAt)
            .ToList();
    }

    public async Task<List<IEgressEvent>> ReadEvents(Int64 sliceId, Int64 start = 0, Int64 end = Int64.MaxValue, CancellationToken cancellationToken = default)
    {
        var sliceBlob = GetSliceBlob(sliceId);
        using var stream = await sliceBlob.OpenReadAsync(start, cancellationToken: cancellationToken).ConfigureAwait(false);
        return DecodeEventsFromStream(stream, end);
    }

    private AppendBlobClient GetSliceBlob(Int64 slice) => _client.GetAppendBlobClient($"{_sliceBlobPathPrefix}{slice.ToPaddedString()}{_sliceBlobPathPostfix}");

    private async Task<AppendBlobClient> GetCurrentSliceBlob(CancellationToken cancellationToken)
    {
        for (var sliceId = _currentSliceId; sliceId < Int64.MaxValue; sliceId++)
        {
            var blob = GetSliceBlob(sliceId);
            await blob.CreateIfNotExistsAsync(httpHeaders: SliceBlobHeaders, cancellationToken: cancellationToken).ConfigureAwait(false);
            _currentSliceId = sliceId;

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
            stream.Write(FieldSeparatorBytes);

            var typeName = _eventTypeResolver.Encode(evt.Payload.GetType());
            stream.WriteAsUtf8(typeName);
            stream.Write(FieldSeparatorBytes);

            stream.WriteAsUtf8(at);
            stream.Write(FieldSeparatorBytes);

            if (evt.Payload is null) throw new BadEventException("One or more provided payloads are null");
            var payloadEncoded = JsonSerializer.Serialize(evt.Payload, SerializationOptions);
            stream.WriteAsUtf8(payloadEncoded);
            stream.Write(FieldSeparatorBytes);

            var metadataEncoded = JsonSerializer.Serialize(evt.Metadata, SerializationOptions);
            stream.WriteAsUtf8(metadataEncoded);
            stream.Write(RecordSeparatorBytes);
        }

        stream.Rewind();
        return stream;
    }

    private List<IEgressEvent> DecodeEventsFromStream(Stream stream, Int64 end)
    {
        var reader = new StreamLineReader(stream);
        var output = new List<IEgressEvent>();

        while (reader.Position < end && reader.TryReadLine() is { } line)
        {
            if (line.Length == 0) continue; // Skip blank lines - used in testing
            var tokens = line.Split(FieldSeparator);
            var aggregateId = tokens[0];
            var typeName = tokens[1];
            var type = _eventTypeResolver.TryDecode(typeName) ?? typeof(Object);
            var at = DateTime.ParseExact(tokens[2], DateFormatString, CultureInfo.InvariantCulture);
            var payload = JsonSerializer.Deserialize(tokens[3], type, SerializationOptions) ?? throw new NeverException();
            var metadata = JsonSerializer.Deserialize<Dictionary<String, String>>(tokens[4], SerializationOptions) ?? new Dictionary<String, String>();

            output.Add(new EgressEvent
            {
                AggregateId = aggregateId,
                Type = typeName,
                Payload = payload,
                Metadata = metadata,
                At = at,
            });
        }

        return output;
    }
}