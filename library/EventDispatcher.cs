using System.Diagnostics;

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

#pragma warning disable CA1031 // Allow catching Exception
            if (type is not null && specificEventHandlers.TryGetValue(type, out var specificEventHandler))
            {
                try
                {
                    specificEventHandler.Invoke(record.Event, record);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex);
                    Console.Error.WriteLine(ex);
                }
            }
            else
            {
                try
                {
                    otherEventHandler.Invoke(record.Event, record);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex);
                    Console.Error.WriteLine(ex);
                }
            }

            try
            {
                anyEventHandler.Invoke(record.Event, record);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex);
                Console.Error.WriteLine(ex);
            }
#pragma warning restore CA1031
        }
    }
}