// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Collections.ObjectModel;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.Utilities;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TProjection>(UInt32 version) : IProjectionBuilder where TProjection : new()
{
    public String Id { get; } = ProjectionIdUtilities.Compute(typeof(TProjection), version);
    public Dictionary<Type, Collection<Object>> EventHandlers { get; } = new();

    public ProjectionConfiguration<TProjection> On<TEventBody>(Action<TProjection, TEventBody, Event> action) where TEventBody : IEventBody
    {
        var eventBodyType = typeof(TEventBody);
        if (!EventHandlers.TryGetValue(eventBodyType, out var actions)) EventHandlers[eventBodyType] = actions = new();
        actions.Add(action);
        return this;
    }

    public Collection<Object> FallbackHandlers { get; } = [];

    public ProjectionConfiguration<TProjection> OnOther(Action<TProjection, Event> action)
    {
        FallbackHandlers.Add(action);
        return this;
    }

    public Collection<Object> PromiscuousHandlers { get; } = [];

    public ProjectionConfiguration<TProjection> OnAny(Action<TProjection, Event> action)
    {
        PromiscuousHandlers.Add(action);
        return this;
    }


    public Dictionary<String, Object> Indexes { get; } = new();

    public ProjectionConfiguration<TProjection> Index(String key, Func<TProjection, String> value)
    {
        if (!Indexes.TryAdd(key, value)) throw new AlreadyExistsException();
        return this;
    }
}

public interface IProjectionBuilder
{
    public String Id { get; }
    Dictionary<Type, Collection<Object>> EventHandlers { get; }
    Collection<Object> FallbackHandlers { get; }
    Collection<Object> PromiscuousHandlers { get; }
    Dictionary<String, Object> Indexes { get; }
}