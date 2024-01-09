namespace ServcoX.EventSauce.Models;

public interface IEgressEvent : IEvent
{
    String Type { get; }
    DateTime At { get; }
    Int64 SliceId { get; } // TODO: Is this needed?
    Int64 Offset { get; } // TODO: Is this needed?
    Int64 NextOffset { get; } // TODO: Is this needed?
}

public readonly record struct EgressEvent(String AggregateId, String Type, Object Payload, IDictionary<String, String> Metadata, DateTime At, Int64 SliceId, Int64 Offset, Int64 NextOffset)
    : IEgressEvent;