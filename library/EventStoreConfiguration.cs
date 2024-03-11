namespace ServcoX.EventSauce;

public sealed class EventStoreConfiguration
{
    private const Int32 MinBlocksPerSlice = 10;
    private const Int32 MaxBlocksPerSlice = 45_000; // Azure has a limit of 50k, and we need safety buffer for overages
    internal Int32 TargetWritesPerSegment { get; private set; } = Int16.MaxValue;

    public EventStoreConfiguration UseTargetWritesPerSegment(Int32 count)
    {
        if (count is < MinBlocksPerSlice or > MaxBlocksPerSlice) throw new ArgumentOutOfRangeException(nameof(count), $"Must not be less than {MinBlocksPerSlice} or greater than {MaxBlocksPerSlice}");
        TargetWritesPerSegment = count;
        return this;
    }
    
    internal TimeSpan AutoPollInterval { get; private set; } = TimeSpan.Zero;

    public EventStoreConfiguration PollEvery(TimeSpan interval)
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