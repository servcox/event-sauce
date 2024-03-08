namespace ServcoX.EventSauce.V3.Models;

public interface IEvent
{
    String AggregateId { get; }
    IEventPayload Payload { get; }
    IDictionary<String, String> Metadata { get; }
    DateTime? At { get; }
}

public readonly record struct Event(String AggregateId, IEventPayload Payload, IDictionary<String, String> Metadata, DateTime? At = null) : IEvent;