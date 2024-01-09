namespace ServcoX.EventSauce.Events;

public interface IEvent
{
    String AggregateId { get; }
    Object Payload { get; }
    IDictionary<String, String> Metadata { get; }
}

public readonly record struct Event(String AggregateId, Object Payload, IDictionary<String, String> Metadata) : IEvent;