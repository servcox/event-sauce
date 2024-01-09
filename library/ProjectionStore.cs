using System.Collections.Concurrent;
using Azure;
using Salar.Bois.LZ4;
using ServcoX.EventSauce.Configurations;
using Timer = System.Timers.Timer;

namespace ServcoX.EventSauce;

public class ProjectionStore : IDisposable
{
    private sealed class Instance
    {
        public ConcurrentDictionary<String, ConcurrentDictionary<Object, List<String>>> Indexes = new(); // Field => Value => Id
        public ConcurrentDictionary<String, Object> Records = new(); // Id => Projection
    }

    private readonly EventStore _store;
    private readonly ProjectionStoreConfiguration _configuration = new();
    private readonly ConcurrentDictionary<Type, Instance> _instances;
    private readonly Timer _cacheWriteTimer;

    private Boolean IsDisposed { get; set; }

    public ProjectionStore(EventStore store, Action<ProjectionStoreConfiguration>? configure = null)
    {
        _store = store;
        configure?.Invoke(_configuration);
        _instances = new(_configuration.Projections.ToDictionary(
            p => p.Key,
            p => new Instance())
        );

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

    public async Task<TProjection> Read<TProjection>(String aggregateId, CancellationToken cancellationToken = default) =>
        await TryRead<TProjection>(aggregateId, cancellationToken).ConfigureAwait(false) ?? throw new NotFoundException();

    public async Task<TProjection?> TryRead<TProjection>(String aggregateId, CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);

        var instance = InstanceOfType<TProjection>();
        if (!instance.Records.TryGetValue(aggregateId, out var projection)) return default;
        return (TProjection)projection;
    }

    public async Task<List<TProjection>> List<TProjection>(CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);

        var instance = InstanceOfType<TProjection>();
        return instance.Records.Values.Cast<TProjection>().ToList();
    }

    public Task<List<TProjection>> Query<TProjection>(String key, Object value, CancellationToken cancellationToken = default) =>
        Query<TProjection>(new Dictionary<String, Object> { [key] = value }, cancellationToken);

    public async Task<List<TProjection>> Query<TProjection>(IDictionary<String, Object> query, CancellationToken cancellationToken = default)
    {
        if (query is null) throw new ArgumentNullException(nameof(query));

        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);

        var instance = InstanceOfType<TProjection>();

        List<String>? candidate = null;
        foreach (var q in query)
        {
            if (!instance.Indexes.TryGetValue(q.Key, out var index)) throw new MissingIndexException($"No index defined on {typeof(TProjection).FullName}.{q.Key}");
            if (!index.TryGetValue(q.Value, out var matches)) return [];

            candidate = candidate is null ? matches : candidate.Intersect(matches).ToList();
            if (candidate.Count == 0) return [];
        }

        return candidate is null ? [] : candidate.Select(id => (TProjection)instance.Records[id]).ToList();
    }

    private void LoadRemoteCache()
    {
        var serializer = new BoisLz4Serializer();
        foreach (var (type, builder) in _configuration.Projections)
        {
            var blob = _store.UnderlyingContainerClient.GetBlobClient($"{_store.AggregateName}/projection/{builder.Id}.bois.lz4");
            try
            {
                var content = blob.DownloadContent();
                using var stream = content.Value.Content.ToStream();
                var instance = serializer.Unpickle<Instance>(stream);
                _instances[type] = instance;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "BlobNotFound")
            {
            }
        }
    }

    private void WriteRemoteCacheIfDirtyCache()
    {
        // TODO: If dirty

        var serializer = new BoisLz4Serializer();
        foreach (var (type, instance) in _instances)
        {
            using var stream = new MemoryStream();
            serializer.Pickle(instance, stream);
            stream.Rewind();

            var projectionId = _configuration.Projections[type].Id;

            var blob = _store.UnderlyingContainerClient.GetBlobClient($"{_store.AggregateName}/projection/{projectionId}.bois.lz4");
            blob.Upload(stream, overwrite: true);
        }
    }

    private async Task LoadAnyNewEvents(CancellationToken cancellationToken)
    {
        // TODO: Fetch new events
        // TODO: Regenerate index
        // TODO: If changed, trigger timer to update cache
        throw new NotImplementedException();
    }

    private Instance InstanceOfType<TProjection>()
    {
        var type = typeof(TProjection);
        if (!_instances.TryGetValue(type, out var instance)) throw new MissingProjectionException($"No projection defined for {type.FullName}");
        return instance;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(Boolean disposing)
    {
        if (IsDisposed) return;
        if (disposing)
        {
            _cacheWriteTimer.Dispose();
        }

        IsDisposed = true;
    }
}