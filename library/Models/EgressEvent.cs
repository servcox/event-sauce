namespace ServcoX.EventSauce.Models;

public interface IEgressEvent : IEvent
{
    String Type { get; }
    DateTime At { get; }
}

public readonly record struct EgressEvent(String AggregateId, String Type, Object Payload, IDictionary<String, String> Metadata, DateTime At) : IEgressEvent;