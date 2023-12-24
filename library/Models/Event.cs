using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Models;

public readonly record struct Event(String StreamId, UInt64 Version, String Type, IEventBody? Body, String CreatedBy, DateTime CreatedAt)
{
    public static Event CreateFrom(EventRecord record, IEventBody? body)
    {
        ArgumentNullException.ThrowIfNull(record);
        
        return new()
        {
            StreamId = record.StreamId,
            Version = record.Version,
            Type = record.Type,
            Body = body,
            CreatedBy = record.CreatedBy,
            CreatedAt = record.Timestamp?.UtcDateTime ?? new DateTime(),
        };
    }
}