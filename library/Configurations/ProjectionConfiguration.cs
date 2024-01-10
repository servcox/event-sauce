// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable ClassNeverInstantiated.Global

using System.Reflection;
using ServcoX.EventSauce.Models;

namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionConfiguration<TAggregate> where TAggregate : new()
{
    public Boolean SyncBeforeReadEnabled { get; private set; } = true;

    public ProjectionConfiguration<TAggregate> DoNotSyncBeforeReads()
    {
        SyncBeforeReadEnabled = false;
        return this;
    }

    public TimeSpan CacheUpdateInterval { get; private set; } = TimeSpan.FromMinutes(15);

    public ProjectionConfiguration<TAggregate> WriteRemoteCacheEvery(TimeSpan interval)
    {
        CacheUpdateInterval = interval;
        return this;
    }

    private GenericAction _creationHandler = new(new Action<TAggregate, IEgressEvent>((_, _) => { }));
    public GenericAction CreationHandler => _creationHandler;

    public ProjectionConfiguration<TAggregate> OnCreation(Action<TAggregate, String> action)
    {
        _creationHandler = new(action);
        return this;
    }

    private readonly Dictionary<Type, GenericAction> _specificEventHandlers = [];
    public IReadOnlyDictionary<Type, GenericAction> SpecificEventHandlers => _specificEventHandlers;

    public ProjectionConfiguration<TAggregate> OnEvent<TEventBody>(Action<TAggregate, TEventBody, IEgressEvent> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));

        var type = typeof(TEventBody);
        _specificEventHandlers[type] = new(action);
        return this;
    }

    private GenericAction _unexpectedEventHandler = new(new Action<TAggregate, IEgressEvent>((_, _) => { }));
    public GenericAction UnexpectedEventHandler => _unexpectedEventHandler;

    public ProjectionConfiguration<TAggregate> OnUnexpectedEvent(Action<TAggregate, IEgressEvent> action)
    {
        _unexpectedEventHandler = new(action);
        return this;
    }

    private GenericAction _anyEventHandler = new(new Action<TAggregate, IEgressEvent>((_, _) => { }));
    public GenericAction AnyEventHandler => _anyEventHandler;

    public ProjectionConfiguration<TAggregate> OnAnyEvent(Action<TAggregate, IEgressEvent> action)
    {
        _anyEventHandler = new(action);
        return this;
    }

    private readonly Dictionary<String, MethodInfo> _indexes = [];
    public IReadOnlyDictionary<String, MethodInfo> Indexes => _indexes;

    public ProjectionConfiguration<TAggregate> IndexField(String fieldName)
    {
        var type = typeof(TAggregate);
        _indexes[fieldName] = type.GetMethod($"get_{fieldName}", BindingFlags.Public | BindingFlags.Instance) ?? throw new InvalidIndexNameException(fieldName);
        return this;
    }
}