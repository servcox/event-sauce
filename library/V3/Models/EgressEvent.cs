namespace ServcoX.EventSauce.V3.Models;

public interface IEgressEvent
{
    String AggregateId { get; }
    Object Payload { get; } // Not IPayload, as we may have an unexpected event in the stream
    IDictionary<String, String> Metadata { get; }
    
    String Type { get; }
    DateTime At { get; }
}

public readonly record struct EgressEvent(String AggregateId, String Type, Object Payload, IDictionary<String, String> Metadata, DateTime At) : IEgressEvent;