// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Collections.ObjectModel;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.Utilities;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TProjection>(UInt32 version) : IProjectionBuilder where TProjection : new()
{
    public String Key { get; } = ProjectionIdUtilities.Compute(typeof(TProjection), version);
    public Dictionary<Type, Collection<Object>> EventHandlers { get; } = new();
    public Collection<Object> FallbackHandlers { get; } = [];
    public Collection<Object> PromiscuousHandlers { get; } = [];

    public ProjectionConfiguration<TProjection> On<TEventBody>(Action<TProjection, TEventBody, Event> action) where TEventBody : IEventBody
    {
        var eventBodyType = typeof(TEventBody);
        if (!EventHandlers.TryGetValue(eventBodyType, out var actions)) EventHandlers[eventBodyType] = actions = new();
        actions.Add(action);
        return this;
    }

    public ProjectionConfiguration<TProjection> OnOther(Action<TProjection, Event> action)
    {
        FallbackHandlers.Add(action);
        return this;
    }

    public ProjectionConfiguration<TProjection> OnAny(Action<TProjection, Event> action)
    {
        PromiscuousHandlers.Add(action);
        return this;
    }
}

public interface IProjectionBuilder
{
    public String Key { get; }
    Dictionary<Type, Collection<Object>> EventHandlers { get; }
    Collection<Object> FallbackHandlers { get; }
    Collection<Object> PromiscuousHandlers { get; }
}