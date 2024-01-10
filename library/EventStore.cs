using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;
using Stream = System.IO.Stream;
using Timer = System.Timers.Timer;

// ReSharper disable ClassWithVirtualMembersNeverInherited.Global
// ReSharper disable UseAwaitUsing
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce;

public class EventStore : IDisposable
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
    private readonly BlobContainerClient _client;
    private readonly Timer? _syncTimer;

    private Int64 _currentSliceId;
    private Boolean _isDisposed;

    public EventStore(String connectionString, String containerName, String aggregateName, Action<EventStoreConfiguration>? builder = null) : this(new(connectionString, containerName), aggregateName,
        builder)
    {
    }

    public EventStore(BlobContainerClient client, String aggregateName, Action<EventStoreConfiguration>? builder = null)
    {
        if (aggregateName is null || !AggregateNamePattern.IsMatch(aggregateName)) throw new ArgumentException($"Must not be null and match pattern {AggregateNamePattern}", nameof(aggregateName));
        _sliceBlobPathPrefix = $"{aggregateName}/event/{aggregateName}.";
        _client = client;
        builder?.Invoke(_configuration);

        _currentSliceId = ListSlices().Result.LastOrDefault().Id;
        
        if (_configuration.SyncInterval.HasValue)
        {
            _syncTimer = new (_configuration.SyncInterval.Value);
            _syncTimer.Elapsed += async (_, _) =>
            {
                await Sync().ConfigureAwait(false);
                _syncTimer.Start();
            };
            _syncTimer.AutoReset = false;
            _syncTimer.Start();
        }
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
            var end = sliceBlob.Properties.ContentLength ?? 0;
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

    private readonly ConcurrentDictionary<Type, IProjection> _projections = new();

    public Projection<TProjection> Project<TProjection>(Int64 version, Action<ProjectionConfiguration<TProjection>> builder) where TProjection : new()
    {
        var type = typeof(TProjection);
        if (_projections.ContainsKey(type)) throw new InvalidOperationException($"Projection of type {typeof(TProjection).FullName} already exists");

        if (builder is null) throw new ArgumentNullException(nameof(builder));
        var configuration = new ProjectionConfiguration<TProjection>();
        builder(configuration);
        var projection = new Projection<TProjection>(version, this, _configuration.SyncBeforeRead, configuration);

        _syncLock.Wait();
        try
        {
            foreach (var (sliceId, remoteEnd) in _localEnds)
            {
                var events = ReadEvents(sliceId, 0, remoteEnd).Result; // TODO: Only read events once for all projections. Cache?
                projection.ApplyEvents(events);
            }

            _projections[type] = projection;
        }
        finally
        {
            _syncLock.Release();
        }

        return projection;
    }

    private readonly Dictionary<Int64, Int64> _localEnds = new(); // SliceId => End;

    private readonly SemaphoreSlim _syncLock = new(1);

    public async Task Sync(CancellationToken cancellationToken = default)
    {
        await _syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var slices = await ListSlices().ConfigureAwait(false);
            foreach (var (sliceId, remoteEnd, _) in slices)
            {
                _localEnds.TryGetValue(sliceId, out var localEnd);
                if (localEnd >= remoteEnd) continue;
                _localEnds[sliceId] = remoteEnd;

                var events = await ReadEvents(sliceId, localEnd, remoteEnd, cancellationToken).ConfigureAwait(false);
                foreach (var (_, projection) in _projections) projection.ApplyEvents(events);
            }
        }
        finally
        {
            _syncLock.Release();
        }
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

            stream.WriteAsUtf8(at);
            stream.Write(FieldSeparatorBytes);

            var typeName = _eventTypeResolver.Encode(evt.Payload.GetType());
            stream.WriteAsUtf8(typeName);
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
            var at = DateTime.ParseExact(tokens[1], DateFormatString, CultureInfo.InvariantCulture);
            var typeName = tokens[2];
            var type = _eventTypeResolver.TryDecode(typeName) ?? typeof(Object);
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

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(Boolean disposing)
    {
        if (_isDisposed) return;
        if (disposing)
        {
            _syncLock.Dispose();
            _syncTimer?.Dispose();
        }

        _isDisposed = true;
    }
}