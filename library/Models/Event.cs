namespace ServcoX.EventSauce.Models;

public interface IEvent
{
    String AggregateId { get; }
    IEventPayload EventPayload { get; }
    IDictionary<String, String> Metadata { get; }
}

public readonly record struct Event(String AggregateId, IEventPayload EventPayload, IDictionary<String, String> Metadata) : IEvent;