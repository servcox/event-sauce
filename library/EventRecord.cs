namespace ServcoX.EventSauce;

public readonly record struct EventRecord(
    DateTime At,
    EventType Type,
    Object Event,
    Int64 StartPosition,
    Int64 Length) : IEventMetadata;

public interface IEventMetadata
{
    DateTime At { get; }
    EventType Type { get; }
}