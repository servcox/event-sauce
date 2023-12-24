using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Models;

public readonly record struct EventStream(String Id, String Type, UInt64 LatestVersion, DateTime LatestUpdateAt, Boolean IsArchived)
{
    public static EventStream CreateFrom(EventStreamRecord eventStreamRecord)
    {
        ArgumentNullException.ThrowIfNull(eventStreamRecord);

        return new()
        {
            Id = eventStreamRecord.PartitionKey,
            Type = eventStreamRecord.Type,
            LatestVersion = eventStreamRecord.LatestVersion,
            LatestUpdateAt = eventStreamRecord.Timestamp?.UtcDateTime ?? new DateTime(),
            IsArchived = eventStreamRecord.IsArchived,
        };
    }
}