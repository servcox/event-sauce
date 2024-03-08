namespace ServcoX.EventSauce;

public sealed class ProjectorConfiguration
{
    internal TimeSpan SyncInterval { get; private set; } = TimeSpan.FromMinutes(1);

    public ProjectorConfiguration SyncEvery(TimeSpan interval)
    {
        SyncInterval = interval;
        return this;
    }

    internal readonly Dictionary<Type, GenericAction> EventHandlers = [];

    public ProjectorConfiguration OnEvent<TEvent>(Action<TEvent, IEventMetadata> action)
    {
        if (action is null) throw new ArgumentNullException(nameof(action));

        var type = typeof(TEvent);
        EventHandlers[type] = new(action);
        return this;
    }

    internal GenericAction UnknownEventHandler { get; private set; } = new(new Action<Object, IEventMetadata>((_, _) => { }));

    public ProjectorConfiguration OnUnknownEvent(Action<Object, IEventMetadata> action)
    {
        UnknownEventHandler = new(action);
        return this;
    }

    internal GenericAction AnyEventHandler { get; private set; } = new(new Action<Object, IEventMetadata>((_, _) => { }));

    public ProjectorConfiguration OnAnyEvent(Action<Object, IEventMetadata> action)
    {
        AnyEventHandler = new(action);
        return this;
    }
}