namespace ServcoX.EventSauce.Events;

public interface IEvent<out TMetadata>
{
    Object Payload { get; }
    TMetadata? Metadata { get; }
}

public readonly record struct Event<TMetadata>(Object Payload, TMetadata? Metadata) : IEvent<TMetadata>;
