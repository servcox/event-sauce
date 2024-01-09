namespace ServcoX.EventSauce.Events;

public interface IEgressEvent : IEvent
{
    String Type { get; }
    DateTime At { get; }
    UInt64 Slice { get; }
    UInt64 Offset { get; }
    UInt64 NextOffset { get; }
}

public readonly record struct EgressEvent(String Type, Object Payload, IDictionary<String,String> Metadata, DateTime At, UInt64 Slice, UInt64 Offset, UInt64 NextOffset) : IEgressEvent;