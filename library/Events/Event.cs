namespace ServcoX.EventSauce.Events;

public interface IEvent
{
    Object Payload { get; }
    IDictionary<String,String> Metadata { get; }
}

public readonly record struct Event(Object Payload, IDictionary<String,String> Metadata) : IEvent;
