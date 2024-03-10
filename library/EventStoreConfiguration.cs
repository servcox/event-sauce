namespace ServcoX.EventSauce;

public sealed class EventStoreConfiguration
{
    internal TimeSpan SyncInterval { get; private set; } = TimeSpan.Zero;

    public EventStoreConfiguration CheckForNewEventsEvery(TimeSpan interval)
    {
        SyncInterval = interval;
        return this;
    }

    internal readonly Dictionary<Type, GenericAction> EventHandlers = [];

    public EventStoreConfiguration OnEvent<TEvent>(Action<TEvent, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        var type = typeof(TEvent);
        EventType.Register(type);
        EventHandlers[type] = new(action);
        return this;
    }

    internal GenericAction OtherEventHandler { get; private set; } = new(new Action<Object, IMetadata>((_, _) => { }));

    public EventStoreConfiguration OnOtherEvent(Action<Object, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        OtherEventHandler = new(action);
        return this;
    }

    internal GenericAction AnyEventHandler { get; private set; } = new(new Action<Object, IMetadata>((_, _) => { }));

    public EventStoreConfiguration OnAnyEvent(Action<Object, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        AnyEventHandler = new(action);
        return this;
    }
}