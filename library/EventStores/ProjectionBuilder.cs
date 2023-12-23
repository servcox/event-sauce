// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Collections.ObjectModel;

namespace ServcoX.EventSauce.EventStores;

public sealed class ProjectionBuilder<TProjection> : IProjectionBuilder where TProjection : new()
{
    public Type Type { get; } = typeof(TProjection);
    public Dictionary<Type, Collection<Object>> EventHandlers { get; } = new();
    public Collection<Object> FallbackHandlers { get; } = [];
    public Collection<Object> PromiscuousHandlers { get; } = [];

    public ProjectionBuilder<TProjection> On<TEventBody>(Action<TProjection, TEventBody, Event> action) where TEventBody : IEventBody
    {
        var eventBodyType = typeof(TEventBody);
        if (!EventHandlers.TryGetValue(eventBodyType, out var actions)) EventHandlers[eventBodyType] = actions = new();
        actions.Add(action);
        return this;
    }

    public ProjectionBuilder<TProjection> OnOther(Action<TProjection, Event> action)
    {
        FallbackHandlers.Add(action);
        return this;
    }

    public ProjectionBuilder<TProjection> OnAny(Action<TProjection, Event> action)
    {
        PromiscuousHandlers.Add(action);
        return this;
    }
}

public interface IProjectionBuilder
{
    Type Type { get; }
    Dictionary<Type, Collection<Object>> EventHandlers { get; }
    Collection<Object> FallbackHandlers { get; }
    Collection<Object> PromiscuousHandlers { get; }
}