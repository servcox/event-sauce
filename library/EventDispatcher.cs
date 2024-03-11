namespace ServcoX.EventSauce;

public static class EventDispatcher
{
    public static void Dispatch(IList<Record> records, IDictionary<Type, GenericAction> specificEventHandlers, Action<Object, IMetadata> otherEventHandler, Action<Object, IMetadata> anyEventHandler)
    {
        ArgumentNullException.ThrowIfNull(records, nameof(records));
        ArgumentNullException.ThrowIfNull(specificEventHandlers, nameof(specificEventHandlers));
        ArgumentNullException.ThrowIfNull(otherEventHandler, nameof(otherEventHandler));
        ArgumentNullException.ThrowIfNull(anyEventHandler, nameof(anyEventHandler));
        
        foreach (var record in records)
        {
            var type = record.Type.TryDecode();

            if (type is not null && specificEventHandlers.TryGetValue(type, out var specificEventHandler))
            {
                specificEventHandler.Invoke(record.Event, record);
            }
            else
            {
                otherEventHandler.Invoke(record.Event, record);
            }

            anyEventHandler.Invoke(record.Event, record);
        }
    }
}