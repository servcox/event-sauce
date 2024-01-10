using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using Azure;
using Azure.Storage.Blobs;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;
using Timer = System.Timers.Timer;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable ClassWithVirtualMembersNeverInherited.Global

namespace ServcoX.EventSauce;

public class Projection<TAggregate> : IProjection, IDisposable where TAggregate : new()
{
    private readonly EventStore _store;
    private readonly BlobContainerClient _client;
    private readonly String _aggregateName;
    private readonly ProjectionConfiguration<TAggregate> _configuration;
    private readonly Timer _cacheWriteTimer;
    private readonly String _projectionId;

    private ProjectionRecord<TAggregate> _record = new();
    private Boolean _remoteCacheDirty;
    private Boolean _isDisposed;

    public Projection(Int64 version, EventStore store, BlobContainerClient client, String aggregateName, ProjectionConfiguration<TAggregate> configuration)
    {
        _store = store;
        _client = client;
        _aggregateName = aggregateName;
        _configuration = configuration;
        _projectionId = ProjectionId.Compute(typeof(TAggregate), version);

        LoadRemoteCache();

        _cacheWriteTimer = new(_configuration.CacheUpdateInterval.TotalMilliseconds);
        _cacheWriteTimer.Elapsed += (_, _) =>
        {
            WriteRemoteCacheIfDirtyCache();
            _cacheWriteTimer.Start();
        };
        _cacheWriteTimer.AutoReset = false;
        _cacheWriteTimer.Start();
    }

    public async Task<TAggregate> Read(String aggregateId, CancellationToken cancellationToken = default) =>
        await TryRead(aggregateId, cancellationToken).ConfigureAwait(false) ?? throw new NotFoundException();

    public async Task<TAggregate?> TryRead(String aggregateId, CancellationToken cancellationToken = default)
    {
        if (_configuration.SyncBeforeReadEnabled) await _store.Sync(cancellationToken).ConfigureAwait(false);
        return _record.Aggregates.GetValueOrDefault(aggregateId);
    }

    public async Task<List<TAggregate>> List(CancellationToken cancellationToken = default)
    {
        if (_configuration.SyncBeforeReadEnabled) await _store.Sync(cancellationToken).ConfigureAwait(false);
        return _record.Aggregates.Values.ToList();
    }

    public Task<List<TAggregate>> Query(String key, String value, CancellationToken cancellationToken = default) =>
        Query(new Dictionary<String, String> { [key] = value }, cancellationToken);

    public async Task<List<TAggregate>> Query(IDictionary<String, String> query, CancellationToken cancellationToken = default)
    {
        if (query is null) throw new ArgumentNullException(nameof(query));

        if (_configuration.SyncBeforeReadEnabled) await _store.Sync(cancellationToken).ConfigureAwait(false);

        List<String>? candidate = null;
        foreach (var q in query)
        {
            if (!_record.Indexes.TryGetValue(q.Key, out var index)) throw new MissingIndexException($"No index defined on {typeof(TAggregate).FullName}.{q.Key}");
            if (!index.TryGetValue(q.Value, out var matches)) return [];

            candidate = candidate is null ? matches : candidate.Intersect(matches).ToList();
            if (candidate.Count == 0) return [];
        }

        return candidate is null ? [] : candidate.Select(id => _record.Aggregates[id]).ToList();
    }

    public void ApplyEvents(List<IEgressEvent> events)
    {
        ProjectEvents(_record.Aggregates, events, _configuration);
        _record.Indexes = GenerateIndex(_record.Aggregates, _configuration);
    }
    
    private static void ProjectEvents(ConcurrentDictionary<String, TAggregate> aggregates, List<IEgressEvent> events, ProjectionConfiguration<TAggregate> configuration)
    {
        if (aggregates is null) throw new ArgumentNullException(nameof(aggregates));
        if (events is null) throw new ArgumentNullException(nameof(events));
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));

        foreach (var evt in events)
        {
            if (!aggregates.TryGetValue(evt.AggregateId, out var aggregate))
            {
                aggregates[evt.AggregateId] = aggregate = new();
                configuration.CreationHandler.Invoke(aggregate, evt.AggregateId);
            }

            Debug.Assert(aggregate != null, nameof(aggregate) + " != null");

            var eventType = EventTypeResolver.Shared.TryDecode(evt.Type);
            if (eventType is not null && configuration.SpecificEventHandlers.TryGetValue(eventType, out var handlers))
            {
                handlers.Invoke(aggregate, evt.Payload, evt);
            }
            else
            {
                configuration.UnexpectedEventHandler.Invoke(aggregate, evt);
            }

            configuration.AnyEventHandler.Invoke(aggregate, evt);
        }
    }

    private static Dictionary<String, Dictionary<String, List<String>>> GenerateIndex(ConcurrentDictionary<String, TAggregate> aggregates, ProjectionConfiguration<TAggregate> configuration)
    {
        if (aggregates is null) throw new ArgumentNullException(nameof(aggregates));
        if (configuration is null) throw new ArgumentNullException(nameof(configuration));

        var indexes = new Dictionary<String, Dictionary<String, List<String>>>(); // Field => Value => Id
        foreach (var (fieldName, method) in configuration.Indexes)
        {
            var index = indexes[fieldName] = new(); // Value => Id

            foreach (var (id, aggregate) in aggregates)
            {
                var value = method.Invoke(aggregate, null);
                if (value is null) continue; // Index does not currently support NULLs
                var valueString = value.ToString()!;
                if (!index.TryGetValue(valueString, out var ids)) ids = index[valueString] = [];
                ids.Add(id);
            }
        }

        return indexes;
    }

    private void LoadRemoteCache()
    {
        var blob = GetCacheBlobClient(_projectionId);
        try
        {
            var content = blob.DownloadContent();
            using var underlyingStream = content.Value.Content.ToStream();
            using var decompressedStream = new BrotliStream(underlyingStream, CompressionMode.Decompress);
            var instance = JsonSerializer.Deserialize<ProjectionRecord<TAggregate>>(decompressedStream)!;
            _record = instance;
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "BlobNotFound")
        {
        }
    }

    private void WriteRemoteCacheIfDirtyCache()
    {
        if (!_remoteCacheDirty) return; // Yep, possible concurrently issue here, but it's only a cache, so not critical
        _remoteCacheDirty = false;

        using var underlyingStream = new MemoryStream();
        using (var compressedStream = new BrotliStream(underlyingStream, CompressionLevel.Optimal, true))
        {
            JsonSerializer.SerializeAsync(compressedStream, _record);
        }

        underlyingStream.Rewind();


        var blob = GetCacheBlobClient(_projectionId);
        blob.Upload(underlyingStream, overwrite: true);
    }

    private BlobClient GetCacheBlobClient(String projectionId) => _client.GetBlobClient($"{_aggregateName}/projection/{projectionId}.json.br");

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
            _cacheWriteTimer.Dispose();
        }

        _isDisposed = true;
    }
}

public interface IProjection
{
    void ApplyEvents(List<IEgressEvent> events);
}

public class ProjectionRecord<TProjection>
{
    public Dictionary<String, Dictionary<String, List<String>>> Indexes { get; set; } = new(); // Field => Value => Id
    public ConcurrentDictionary<String, TProjection> Aggregates { get; init; } = new(); // Id => Projection
}