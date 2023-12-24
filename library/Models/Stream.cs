using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Models;

public readonly record struct Stream(String Id, String Type, UInt64 LatestVersion, DateTime LatestUpdateAt, Boolean IsArchived)
{
    public static Stream CreateFrom(StreamRecord streamRecord)
    {
        ArgumentNullException.ThrowIfNull(streamRecord);

        return new()
        {
            Id = streamRecord.StreamId,
            Type = streamRecord.Type,
            LatestVersion = streamRecord.LatestVersion,
            LatestUpdateAt = streamRecord.Timestamp?.UtcDateTime ?? new DateTime(),
            IsArchived = streamRecord.IsArchived,
        };
    }
}