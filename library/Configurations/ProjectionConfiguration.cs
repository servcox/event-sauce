using System.Collections.ObjectModel;
using ServcoX.EventSauce.Utilities;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TProjection>(String streamType, UInt32 version) : IProjectionBuilder where TProjection : new()
{
    public String StreamType { get; } = streamType.ToUpperInvariant();
    public String Id { get; } = ProjectionIdUtilities.Compute(typeof(TProjection), version);

    public Collection<Object> CreationHandlers { get; } = new();

    public ProjectionConfiguration<TProjection> OnCreation(Action<TProjection, String> action)
    {
        CreationHandlers.Add(action);
        return this;
    }

    public Dictionary<Type, Collection<Object>> EventHandlers { get; } = new();

    public ProjectionConfiguration<TProjection> OnEvent<TEventBody>(Action<TProjection, TEventBody, Event> action) where TEventBody : IEventBody
    {
        var eventBodyType = typeof(TEventBody);
        if (!EventHandlers.TryGetValue(eventBodyType, out var actions)) EventHandlers[eventBodyType] = actions = new();
        actions.Add(action);
        return this;
    }

    public Collection<Object> FallbackHandlers { get; } = [];

    public ProjectionConfiguration<TProjection> OnUnexpectedEvent(Action<TProjection, Event> action)
    {
        FallbackHandlers.Add(action);
        return this;
    }

    public Collection<Object> PromiscuousHandlers { get; } = [];

    public ProjectionConfiguration<TProjection> OnAnyEvent(Action<TProjection, Event> action)
    {
        PromiscuousHandlers.Add(action);
        return this;
    }

    public Dictionary<String, Object> Indexes { get; } = new();

    public ProjectionConfiguration<TProjection> Index(String key, Func<TProjection, String> value)
    {
        if (Indexes.ContainsKey(key)) throw new AlreadyExistsException();
        Indexes.Add(key, value);
        return this;
    }
}

public interface IProjectionBuilder
{
    String StreamType { get; }
    String Id { get; }
    Collection<Object> CreationHandlers { get; }
    Dictionary<Type, Collection<Object>> EventHandlers { get; }
    Collection<Object> FallbackHandlers { get; }
    Collection<Object> PromiscuousHandlers { get; }
    Dictionary<String, Object> Indexes { get; }
}