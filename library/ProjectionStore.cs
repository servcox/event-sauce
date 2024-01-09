using System.Collections.Concurrent;
using ServcoX.EventSauce.Configurations;

namespace ServcoX.EventSauce;

public class ProjectionStore
{
    private sealed class Instance(IProjectionBuilder builder)
    {
        public readonly IProjectionBuilder Builder = builder;
        public ConcurrentDictionary<String, ConcurrentDictionary<Object, List<String>>> Indexes = new(); // Field => Value => Id
        public ConcurrentDictionary<String, Object> Records = new(); // Id => Projection
    }

    private readonly EventStore _store;
    private readonly ProjectionStoreConfiguration _configuration = new();
    private readonly Dictionary<Type, Instance> _instances; // Treat read-only, however not defined as such for performance

    public ProjectionStore(EventStore store, Action<ProjectionStoreConfiguration>? configure = null)
    {
        _store = store;
        configure?.Invoke(_configuration);
        _instances = _configuration.Projections.ToDictionary(p => p.Key, p => new Instance(p.Value));

        LoadRemoteCache();
        // TODO: await WriteRemoteCacheIfDirty(cancellationToken).ConfigureAwait(false);
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
        throw new NotImplementedException();
    }

    private async Task WriteRemoteCacheIfDirty(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    private async Task WriteCache(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    private async Task LoadAnyNewEvents(CancellationToken cancellationToken)
    {
        //https://github.com/salarcode/Bois
        // Container.GetBlobClient($"{_aggregateName}/projection/{projectionKey}.bois.lz4");

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
}