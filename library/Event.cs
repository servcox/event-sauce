using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce;

public record struct Event(String StreamId, UInt64 Version, String Type, IEventBody? Body, String CreatedBy, DateTime CreatedAt)
{
    public static Event CreateFrom(EventRecord record, IEventBody? body)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        
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