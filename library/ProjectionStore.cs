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
        public Dictionary<String, Dictionary<Object, List<String>>> Indexes = new(); // Field => Value => Id
        public readonly ConcurrentDictionary<String, Object> Aggregates = new(); // Id => Projection
    }

    private readonly EventStore _store;
    private readonly ProjectionStoreConfiguration _configuration = new();
    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly ConcurrentDictionary<Type, Instance> _instances;
    private readonly Timer _cacheWriteTimer;

    private Boolean _isDisposed;
    private Boolean _remoteCacheDirty;

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
        if (_configuration.CacheUpdateInterval.TotalMilliseconds > 0) _cacheWriteTimer.Start();
    }

    public async Task<TProjection> Read<TProjection>(String aggregateId, CancellationToken cancellationToken = default) =>
        await TryRead<TProjection>(aggregateId, cancellationToken).ConfigureAwait(false) ?? throw new NotFoundException();

    public async Task<TProjection?> TryRead<TProjection>(String aggregateId, CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);

        var instance = InstanceOfType<TProjection>();
        if (!instance.Aggregates.TryGetValue(aggregateId, out var projection)) return default;
        return (TProjection)projection;
    }

    public async Task<List<TProjection>> List<TProjection>(CancellationToken cancellationToken = default)
    {
        await LoadAnyNewEvents(cancellationToken).ConfigureAwait(false);

        var instance = InstanceOfType<TProjection>();
        return instance.Aggregates.Values.Cast<TProjection>().ToList();
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

        return candidate is null ? [] : candidate.Select(id => (TProjection)instance.Aggregates[id]).ToList();
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

    private readonly ConcurrentDictionary<Int64, Int64> _localNextOffsets = new(); // Slice => Offset;

    private async Task LoadAnyNewEvents(CancellationToken cancellationToken)
    {
        // TODO: Lock
        var slices = await _store.ListSlices().ConfigureAwait(false);
        foreach (var (sliceId, remoteNextOffset, _) in slices)
        {
            _localNextOffsets.TryGetValue(sliceId, out var localNextOffset);
            if (localNextOffset >= remoteNextOffset) continue;

            var events = await _store.ReadEvents(sliceId, localNextOffset, remoteNextOffset, cancellationToken).ConfigureAwait(false);
            foreach (var (type, instance) in _instances)
            {
                var builder = _configuration.Projections[type];

                foreach (var evt in events)
                {
                    if (!instance.Aggregates.TryGetValue(evt.AggregateId, out var aggregate))
                    {
                        instance.Aggregates[evt.AggregateId] = aggregate = Activator.CreateInstance(type)!;
                        foreach (var handler in builder.CreationHandlers)
                        {
                            var method = handler
                                .GetType()
                                .GetMethod(nameof(Action.Invoke)) ?? throw new NeverException(); // TODO: Better performance by precomputing this?
                            method.Invoke(handler, new[] { aggregate, evt.AggregateId });
                        }
                    }

                    var specificHandlerFound = false;
                    var eventType = _eventTypeResolver.TryDecode(evt.Type);
                    if (eventType is not null)
                    {
                        if (builder.SpecificEventHandlers.TryGetValue(eventType, out var handlers))
                        {
                            specificHandlerFound = true;
                            foreach (var handler in handlers)
                            {
                                var method = handler
                                    .GetType()
                                    .GetMethod(nameof(Action.Invoke)) ?? throw new NeverException();
                                method.Invoke(handler, new[] { aggregate, evt.Payload, evt });
                            }
                        }
                    }

                    if (!specificHandlerFound)
                    {
                        foreach (var handler in builder.UnexpectedEventHandlers)
                        {
                            var method = handler
                                .GetType()
                                .GetMethod(nameof(Action.Invoke)) ?? throw new NeverException(); // TODO: Better performance by precomputing this?
                            method.Invoke(handler, new[] { aggregate, evt });
                        }
                    }

                    foreach (var handler in builder.AnyEventHandlers)
                    {
                        var method = handler
                            .GetType()
                            .GetMethod(nameof(Action.Invoke)) ?? throw new NeverException(); // TODO: Better performance by precomputing this?
                        method.Invoke(handler, new[] { aggregate, evt });
                    }
                }

                var indexes = new Dictionary<String, Dictionary<Object, List<String>>>(); // Field => Value => Id
                foreach (var indexFieldName in builder.Indexes)
                {
                    var index = indexes[indexFieldName] = new(); // Value => Id

                    var field = builder.Type.GetField(indexFieldName) ?? throw new InvalidIndexNameException(indexFieldName);

                    foreach (var (id, aggregate) in instance.Aggregates)
                    {
                        var fieldValue = field.GetValue(aggregate);
                        if (fieldValue is null) continue; // Index does not currently support NULLs
                        if (!index.TryGetValue(fieldValue, out var ids)) ids = index[fieldValue] = [];
                        ids.Add(id);
                    }
                }

                instance.Indexes = indexes;
            }

            _localNextOffsets[sliceId] = remoteNextOffset;
            if (events.Count > 0) _remoteCacheDirty = true;
        }
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
        if (_isDisposed) return;
        if (disposing)
        {
            _cacheWriteTimer.Dispose();
        }

        _isDisposed = true;
    }
}