namespace ServcoX.EventSauce;

public readonly record struct Record(
    DateTime At,
    EventType Type,
    Object Event) : IMetadata;
    