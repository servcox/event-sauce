using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce;

public record struct Stream(String Id, String Type, UInt64 LatestVersion, DateTime LatestUpdateAt, Boolean IsArchived)
{
    public static Stream CreateFrom(StreamRecord record)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));

        return new()
        {
            Id = record.StreamId,
            Type = record.Type,
            LatestVersion = record.LatestVersion,
            LatestUpdateAt = record.Timestamp?.UtcDateTime ?? new DateTime(),
            IsArchived = record.IsArchived,
        };
    }
}