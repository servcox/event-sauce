using System.Collections.Concurrent;
using ServcoX.EventSauce.ConfigurationBuilders;

namespace ServcoX.EventSauce;

public class ProjectionStore
{
    private readonly EventStore _store;
    private readonly BaseProjectionConfiguration _configuration = new();
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<String, Object>> _projections = new(); // ProjectionType => ID => Body
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<String, ConcurrentDictionary<Object, List<String>>>> _indexes = new(); // ProjectionType => Field => Value => ID

    public ProjectionStore(EventStore store, Action<BaseProjectionConfiguration>? configure = null)
    {
        _store = store;
        configure?.Invoke(_configuration);

        LoadRemoteCache();
    }

    public async Task<TProjection> Read<TProjection>(String streamId, CancellationToken cancellationToken = default) =>
        await TryRead<TProjection>(streamId, cancellationToken).ConfigureAwait(false) ?? throw new NotFoundException();

    public async Task<TProjection?> TryRead<TProjection>(String streamId, CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);
        await WriteRemoteCacheIfDirty(cancellationToken).ConfigureAwait(false);

        var type = typeof(TProjection);
        if (!_projections.TryGetValue(type, out var projections)) throw new MissingProjectionException($"No projection defined for {type.FullName}");
        if (!projections.TryGetValue(streamId, out var projection)) return default;
        return (TProjection)projection;
    }

    public async Task<List<TProjection>> List<TProjection>(CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);
        await WriteRemoteCacheIfDirty(cancellationToken).ConfigureAwait(false);

        var type = typeof(TProjection);
        if (!_projections.TryGetValue(type, out var projections)) throw new MissingProjectionException($"No projection defined for {type.FullName}");

        return projections.Values.Cast<TProjection>().ToList();
    }

    public Task<List<TProjection>> Query<TProjection>(String key, Object value, CancellationToken cancellationToken = default) =>
        Query<TProjection>(new Dictionary<String, Object> { [key] = value }, cancellationToken);

    public async Task<List<TProjection>> Query<TProjection>(IDictionary<String, Object> query, CancellationToken cancellationToken = default)
    {
        if (query is null) throw new ArgumentNullException(nameof(query));

        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);
        await WriteRemoteCacheIfDirty(cancellationToken).ConfigureAwait(false);

        var type = typeof(TProjection);
        if (!_indexes.TryGetValue(type, out var indexes)) throw new MissingProjectionException($"No projection defined for {type.FullName}");

        List<String>? candidate = null;
        foreach (var q in query)
        {
            if (!indexes.TryGetValue(q.Key, out var index)) throw new MissingIndexException($"No index defined on {type.FullName}.{q.Key}");
            if (!index.TryGetValue(q.Value, out var matches)) return [];

            candidate = candidate is null ? matches : candidate.Intersect(matches).ToList();
            if (candidate.Count == 0) return [];
        }

        if (!_projections.TryGetValue(type, out var projections)) throw new MissingProjectionException($"No projection defined for {type.FullName}");
        return candidate is null ? [] : candidate.Select(id => (TProjection)projections[id]).ToList();
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
        // TODO: Fetch new events
        // TODO: Regenerate index
        // TODO: If changed, trigger timer to update cache
        throw new NotImplementedException();
    }
}