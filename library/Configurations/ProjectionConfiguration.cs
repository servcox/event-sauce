// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using ServcoX.EventSauce.Models;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TProjection>(Int64 version) : IProjectionConfiguration where TProjection : new()
{
    public String Id { get; } = ProjectionId.Compute(typeof(TProjection), version);
    public Type Type { get; } = typeof(TProjection);

    private readonly List<Object> _creationHandlers = [];
    public IReadOnlyList<Object> CreationHandlers => _creationHandlers;

    public ProjectionConfiguration<TProjection> OnCreation(Action<TProjection, String> action)
    {
        _creationHandlers.Add(action);
        return this;
    }

    private readonly Dictionary<Type, List<Object>> _specificEventHandlers = [];
    public IReadOnlyDictionary<Type, IReadOnlyList<Object>> SpecificEventHandlers => _specificEventHandlers.ToDictionary(handler => handler.Key, a => (IReadOnlyList<Object>)a.Value).AsReadOnly();

    public ProjectionConfiguration<TProjection> OnEvent<TEventBody>(Action<TProjection, TEventBody, IEgressEvent> action)
    {
        var eventBodyType = typeof(TEventBody);
        if (!_specificEventHandlers.TryGetValue(eventBodyType, out var actions)) _specificEventHandlers[eventBodyType] = actions = [];
        actions.Add(action);
        return this;
    }

    private readonly List<Object> _unexpectedEventHandlers = [];
    public IReadOnlyList<Object> UnexpectedEventHandlers => _unexpectedEventHandlers;

    public ProjectionConfiguration<TProjection> OnUnexpectedEvent(Action<TProjection, IEgressEvent> action)
    {
        _unexpectedEventHandlers.Add(action);
        return this;
    }

    private readonly List<Object> _anyEventHandlers = [];
    public IReadOnlyList<Object> AnyEventHandlers => _anyEventHandlers;

    public ProjectionConfiguration<TProjection> OnAnyEvent(Action<TProjection, IEgressEvent> action)
    {
        _anyEventHandlers.Add(action);
        return this;
    }

    private readonly HashSet<String> _indexes = [];
    public IReadOnlyList<String> Indexes => new List<String>(_indexes);

    public ProjectionConfiguration<TProjection> IndexField(String key)
    {
        _indexes.Add(key);
        return this;
    }
}

public interface IProjectionConfiguration
{
    String Id { get; }
    Type Type { get; }
    IReadOnlyList<Object> CreationHandlers { get; }
    IReadOnlyDictionary<Type, IReadOnlyList<Object>> SpecificEventHandlers { get; }
    IReadOnlyList<Object> UnexpectedEventHandlers { get; }
    IReadOnlyList<Object> AnyEventHandlers { get; }
    IReadOnlyList<String> Indexes { get; }
}