namespace ServcoX.EventSauce.V3.Configurations;

public sealed class EventStoreConfiguration
{
    private const Int32 MinBlocksPerSlice = 10;

    private const Int32 MaxBlocksPerSlice = 45_000; // Azure has a limit of 50k, and we need safety buffer for overages
    internal Int32 TargetBlocksPerSlice { get; private set; } = 25_000;

    public EventStoreConfiguration UseTargetBlocksPerSlice(Int32 count)
    {
        if (count is < MinBlocksPerSlice or > MaxBlocksPerSlice) throw new ArgumentOutOfRangeException(nameof(count), $"Must not be less than {MinBlocksPerSlice} or greater than {MaxBlocksPerSlice}");
        TargetBlocksPerSlice = count;
        return this;
    }

    internal Boolean SyncBeforeRead { get; private set; } = true;

    public EventStoreConfiguration DoNotSyncBeforeReads()
    {
        SyncBeforeRead = false;
        return this;
    }

    internal TimeSpan? SyncInterval { get; private set; }

    public EventStoreConfiguration SyncEvery(TimeSpan interval)
    {
        SyncInterval = interval;
        return this;
    }
}