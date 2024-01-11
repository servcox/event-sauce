namespace ServcoX.EventSauce.Models;

public interface IEvent
{
    String AggregateId { get; }
    IEventPayload Payload { get; }
    IDictionary<String, String> Metadata { get; }
}

public readonly record struct Event(String AggregateId, IEventPayload Payload, IDictionary<String, String> Metadata) : IEvent;