using System.Globalization;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Models;

public readonly record struct Event(String StreamId, UInt64 Version, String Type, IEventBody? Body, String CreatedBy, DateTime CreatedAt)
{
    public static Event CreateFrom(EventRecord eventRecord, IEventBody? body)
    {
        ArgumentNullException.ThrowIfNull(eventRecord);
        
        return new()
        {
            StreamId = eventRecord.PartitionKey,
            Version = UInt64.Parse(eventRecord.RowKey, CultureInfo.InvariantCulture),
            Type = eventRecord.Type,
            Body = body,
            CreatedBy = eventRecord.CreatedBy,
            CreatedAt = eventRecord.Timestamp?.UtcDateTime ?? new DateTime(),
        };
    }
}