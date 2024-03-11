namespace ServcoX.EventSauce;

public sealed class EventStoreConfiguration
{
    internal TimeSpan AutoPollInterval { get; private set; } = TimeSpan.Zero;

    public EventStoreConfiguration CheckForNewEventsEvery(TimeSpan interval)
    {
        AutoPollInterval = interval;
        return this;
    }

    internal readonly Dictionary<Type, GenericAction> SpecificEventHandlers = [];

    public EventStoreConfiguration OnEvent<TEvent>(Action<TEvent, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        var type = typeof(TEvent);
        EventType.Register(type);
        SpecificEventHandlers[type] = new(action);
        return this;
    }

    internal Action<Object, IMetadata> OtherEventHandler { get; private set; } = (_, _) => { };

    public EventStoreConfiguration OnOtherEvent(Action<Object, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        OtherEventHandler = action;
        return this;
    }

    internal Action<Object, IMetadata> AnyEventHandler { get; private set; } = (_, _) => { };

    public EventStoreConfiguration OnAnyEvent(Action<Object, IMetadata> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        AnyEventHandler = action;
        return this;
    }
}