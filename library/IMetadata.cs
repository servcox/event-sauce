namespace ServcoX.EventSauce;

public interface IMetadata
{
    DateTime At { get; }
    EventType Type { get; }
}