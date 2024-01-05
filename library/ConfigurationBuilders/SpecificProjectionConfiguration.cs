using ServcoX.EventSauce.V2;
using ServcoX.EventSauce.V2.Utilities;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

namespace ServcoX.EventSauce.ConfigurationBuilders;

public sealed class SpecificProjectionConfiguration<TProjection>(String streamType, UInt32 version) : IProjectionBuilder where TProjection : new()
{
    public String StreamType { get; } = streamType.ToUpperInvariant();
    public String Id { get; } = ProjectionIdUtilities.Compute(typeof(TProjection), version);

    private readonly List<Object> _creationHandlers = [];
    public IReadOnlyList<Object> CreationHandlers => _creationHandlers;

    public SpecificProjectionConfiguration<TProjection> OnCreation(Action<TProjection, String> action)
    {
        _creationHandlers.Add(action);
        return this;
    }

    private readonly Dictionary<Type, List<Object>> _specificEventHandlers = [];
    public IReadOnlyDictionary<Type, List<Object>> SpecificEventHandlers => _specificEventHandlers; // TODO: Should return fully read-only type, just not worth the squeeze today

    public SpecificProjectionConfiguration<TProjection> OnEvent<TEventBody>(Action<TProjection, TEventBody, Event> action) where TEventBody : IEventBody
    {
        var eventBodyType = typeof(TEventBody);
        if (!_specificEventHandlers.TryGetValue(eventBodyType, out var actions)) _specificEventHandlers[eventBodyType] = actions = new();
        actions.Add(action);
        return this;
    }

    private readonly List<Object> _unexpectedEventHandlers = [];
    public IReadOnlyList<Object> UnexpectedEventHandlers => _unexpectedEventHandlers;

    public SpecificProjectionConfiguration<TProjection> OnUnexpectedEvent(Action<TProjection, Event> action)
    {
        _unexpectedEventHandlers.Add(action);
        return this;
    }
    
    private readonly List<Object> _anyEventHandlers = [];
    public IReadOnlyList<Object> AnyEventHandlers => _anyEventHandlers;

    public SpecificProjectionConfiguration<TProjection> OnAnyEvent(Action<TProjection, Event> action)
    {
        _anyEventHandlers.Add(action);
        return this;
    }

    // public Dictionary<String, Object> Indexes { get; } = new();
    //
    // public SpecificProjectionConfiguration<TProjection> Index(String key, Func<TProjection, String> value)
    // {
    //     if (Indexes.ContainsKey(key)) throw new AlreadyExistsException();
    //     Indexes.Add(key, value);
    //     return this;
    // }
}

public interface IProjectionBuilder
{
    String StreamType { get; }
    String Id { get; }
    IReadOnlyList<Object> CreationHandlers { get; }
    IReadOnlyDictionary<Type, List<Object>> SpecificEventHandlers { get; }
    IReadOnlyList<Object> UnexpectedEventHandlers { get; }
    IReadOnlyList<Object> AnyEventHandlers { get; }
    // IReadOnlyDictionary<String, Object> Indexes { get; }
}