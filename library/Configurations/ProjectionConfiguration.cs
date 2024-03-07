// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Reflection;
using ServcoX.EventSauce.Models;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TAggregate> where TAggregate : new()
{
    internal GenericAction CreationHandler { get; private set; } = new(new Action<TAggregate, String>((_, _) => { }));

    public ProjectionConfiguration<TAggregate> OnCreation(Action<TAggregate, String> action)
    {
        CreationHandler = new(action);
        return this;
    }

    private readonly Dictionary<Type, GenericAction> _specificEventHandlers = [];
    internal IReadOnlyDictionary<Type, GenericAction> SpecificEventHandlers => _specificEventHandlers;

    public ProjectionConfiguration<TAggregate> OnEvent<TEventBody>(Action<TAggregate, TEventBody, IEgressEvent> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));

        var type = typeof(TEventBody);
        _specificEventHandlers[type] = new(action);
        return this;
    }

    internal GenericAction UnexpectedEventHandler { get; private set; } = new(new Action<TAggregate, IEgressEvent>((_, _) => { }));

    public ProjectionConfiguration<TAggregate> OnUnexpectedEvent(Action<TAggregate, IEgressEvent> action)
    {
        UnexpectedEventHandler = new(action);
        return this;
    }

    internal GenericAction AnyEventHandler { get; private set; } = new(new Action<TAggregate, IEgressEvent>((_, _) => { }));

    public ProjectionConfiguration<TAggregate> OnAnyEvent(Action<TAggregate, IEgressEvent> action)
    {
        AnyEventHandler = new(action);
        return this;
    }

    private readonly Dictionary<String, MethodInfo> _indexes = [];
    internal IReadOnlyDictionary<String, MethodInfo> Indexes => _indexes;

    public ProjectionConfiguration<TAggregate> IndexField(String fieldName)
    {
        var type = typeof(TAggregate);
        _indexes[fieldName] = type.GetMethod($"get_{fieldName}", BindingFlags.Public | BindingFlags.Instance) ?? throw new InvalidIndexNameException(fieldName);
        return this;
    }
}