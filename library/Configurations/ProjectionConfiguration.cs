// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Reflection;
using ServcoX.EventSauce.Models;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TProjection>(Int64 version) : IProjectionConfiguration where TProjection : new()
{
    public String Id { get; } = ProjectionId.Compute(typeof(TProjection), version);
    public Type Type { get; } = typeof(TProjection);

    private GenericAction _creationHandler = new(new Action<TProjection, IEgressEvent>((_, _) => { }));
    public GenericAction CreationHandler => _creationHandler;

    public ProjectionConfiguration<TProjection> OnCreation(Action<TProjection, String> action)
    {
        _creationHandler = new(action);
        return this;
    }

    private readonly Dictionary<Type, GenericAction> _specificEventHandlers = [];
    public IReadOnlyDictionary<Type, GenericAction> SpecificEventHandlers => _specificEventHandlers;

    public ProjectionConfiguration<TProjection> OnEvent<TEventBody>(Action<TProjection, TEventBody, IEgressEvent> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));

        var type = typeof(TEventBody);
        _specificEventHandlers[type] = new(action);
        return this;
    }

    private GenericAction _unexpectedEventHandler =  new(new Action<TProjection, IEgressEvent>((_, _) => { }));
    public GenericAction UnexpectedEventHandler => _unexpectedEventHandler;

    public ProjectionConfiguration<TProjection> OnUnexpectedEvent(Action<TProjection, IEgressEvent> action)
    {
        _unexpectedEventHandler = new(action);
        return this;
    }

    private GenericAction _anyEventHandler =  new(new Action<TProjection, IEgressEvent>((_, _) => { }));
    public GenericAction AnyEventHandler => _anyEventHandler;

    public ProjectionConfiguration<TProjection> OnAnyEvent(Action<TProjection, IEgressEvent> action)
    {
        _anyEventHandler = new(action);
        return this;
    }

    private readonly Dictionary<String, MethodInfo> _indexes = [];
    public IReadOnlyDictionary<String, MethodInfo> Indexes => _indexes;

    public ProjectionConfiguration<TProjection> IndexField(String fieldName)
    {
        _indexes[fieldName] = Type.GetMethod($"get_{fieldName}", BindingFlags.Public | BindingFlags.Instance) ?? throw new InvalidIndexNameException(fieldName);
        return this;
    }
}

public interface IProjectionConfiguration
{
    String Id { get; }
    Type Type { get; }
    GenericAction CreationHandler { get; }
    IReadOnlyDictionary<Type, GenericAction> SpecificEventHandlers { get; }
    GenericAction UnexpectedEventHandler { get; }
    GenericAction AnyEventHandler { get; }
    IReadOnlyDictionary<String, MethodInfo> Indexes { get; }
}