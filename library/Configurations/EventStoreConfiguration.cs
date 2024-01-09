namespace ServcoX.EventSauce.Configurations;

public sealed class EventStoreConfiguration
{
    /// <remarks>Changing on an existing database will cause out-of-order events. Must be less than the Azure limit of 50,000. Maximum of 45,000 recommended as overage is possible</remarks>
    public Int32 TargetBlocksPerSlice { get; private set; } = 25_000;
    public EventStoreConfiguration UseTargetBlocksPerSlice(Int32 count)
    {
        TargetBlocksPerSlice = count;
        return this;
    }
}