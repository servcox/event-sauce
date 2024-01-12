using System.Collections.Concurrent;
using System.Diagnostics;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable ClassWithVirtualMembersNeverInherited.Global

namespace ServcoX.EventSauce;

public class Projection<TAggregate> : IProjection where TAggregate : new()
{
    private readonly EventStore _store;
    private readonly Boolean _syncBeforeReads;
    private readonly ProjectionConfiguration<TAggregate> _configuration;
    private Dictionary<String, Dictionary<String, List<String>>> _indexes = new(); // Field => Value => Id
    private readonly ConcurrentDictionary<String, TAggregate> _aggregates = new(); // Id => Projection

    public Projection(Int64 version, EventStore store, Boolean syncBeforeReads, ProjectionConfiguration<TAggregate> configuration)
    {
        _store = store;
        _syncBeforeReads = syncBeforeReads;
        _configuration = configuration;
        RegenerateIndexes();
    }

    public async Task<TAggregate> Read(String aggregateId, CancellationToken cancellationToken = default) =>
        await TryRead(aggregateId, cancellationToken).ConfigureAwait(false) ?? throw new NotFoundException();

    public async Task<TAggregate?> TryRead(String aggregateId, CancellationToken cancellationToken = default)
    {
        if (_syncBeforeReads) await _store.Sync(cancellationToken).ConfigureAwait(false);
        return _aggregates.GetValueOrDefault(aggregateId);
    }

    public async Task<List<TAggregate>> List(CancellationToken cancellationToken = default)
    {
        if (_syncBeforeReads) await _store.Sync(cancellationToken).ConfigureAwait(false);
        return _aggregates.Values.ToList();
    }

    public Task<List<TAggregate>> List(String filterKey, String filterValue, CancellationToken cancellationToken = default) =>
        List(new Dictionary<String, String> { [filterKey] = filterValue }, cancellationToken);

    public async Task<List<TAggregate>> List(IDictionary<String, String> filters, CancellationToken cancellationToken = default)
    {
        if (filters is null) throw new ArgumentNullException(nameof(filters));

        if (_syncBeforeReads) await _store.Sync(cancellationToken).ConfigureAwait(false);

        if (filters.Count == 0) return await List(cancellationToken).ConfigureAwait(false);

        List<String>? candidate = null;
        foreach (var filter in filters)
        {
            if (!_indexes.TryGetValue(filter.Key, out var index))
                throw new MissingIndexException($"Filter keys must be pre-indexed and there is not am index defined for '{filter.Key}'. Current indexes on '{String.Join(',', _indexes.Keys)}'");
            if (!index.TryGetValue(filter.Value, out var matches)) return [];

            candidate = candidate is null ? matches : candidate.Intersect(matches).ToList();
            if (candidate.Count == 0) return [];
        }

        return candidate is null ? [] : candidate.Select(id => _aggregates[id]).ToList();
    }

    public void ApplyEvents(List<IEgressEvent> events)
    {
        ProjectEvents(events);
        RegenerateIndexes();
    }

    private void ProjectEvents(List<IEgressEvent> events)
    {
        if (events is null) throw new ArgumentNullException(nameof(events));

        foreach (var evt in events)
        {
            if (!_aggregates.TryGetValue(evt.AggregateId, out var aggregate))
            {
                _aggregates[evt.AggregateId] = aggregate = new();
                _configuration.CreationHandler.Invoke(aggregate, evt.AggregateId);
            }

            Debug.Assert(aggregate != null, nameof(aggregate) + " != null");

            var eventType = EventTypeResolver.Shared.TryDecode(evt.Type);
            if (eventType is not null && _configuration.SpecificEventHandlers.TryGetValue(eventType, out var handlers))
            {
                handlers.Invoke(aggregate, evt.Payload, evt);
            }
            else
            {
                _configuration.UnexpectedEventHandler.Invoke(aggregate, evt);
            }

            _configuration.AnyEventHandler.Invoke(aggregate, evt);
        }
    }

    private void RegenerateIndexes()
    {
        var indexes = new Dictionary<String, Dictionary<String, List<String>>>(); // Field => Value => Id
        foreach (var (fieldName, method) in _configuration.Indexes)
        {
            var index = indexes[fieldName] = new(); // Value => Id

            foreach (var (id, aggregate) in _aggregates)
            {
                var value = method.Invoke(aggregate, null);
                var valueString = value?.ToString();
                if (valueString is null) continue; // Index does not currently support NULLs
                if (!index.TryGetValue(valueString, out var ids)) ids = index[valueString] = [];
                ids.Add(id);
            }
        }

        _indexes = indexes;
    }
}

public interface IProjection
{
    void ApplyEvents(List<IEgressEvent> events);
}