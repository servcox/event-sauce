namespace ServcoX.EventSauce.Events;

public interface IEgressEvent<out TMetadata> : IEvent<TMetadata>
{
    String Type { get; }
    DateTime At { get; }
    UInt64 Slice { get; }
    UInt64 StartOffset { get; }
    UInt64 EndOffset { get; }
}

public readonly record struct EgressEvent<TMetadata>(String Type, Object Payload, TMetadata? Metadata, DateTime At, UInt64 Slice, UInt64 StartOffset, UInt64 EndOffset) : IEgressEvent<TMetadata>;