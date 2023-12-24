using Azure;
using Azure.Data.Tables;

namespace ServcoX.EventSauce.TableRecords;

public sealed class EventStreamRecord : ITableEntity
{
    public EventStreamRecord()
    {
        PartitionKey = String.Empty;
        RowKey = String.Empty;
        Type = String.Empty;
    }

    public EventStreamRecord(String streamId, String type, UInt64 latestVersion, Boolean isArchived)
    {
        PartitionKey = streamId;
        RowKey = streamId;
        Type = type;
        LatestVersion = latestVersion;
        IsArchived = isArchived;
    }

    public String Type { get; set; }
    public UInt64 LatestVersion { get; set; }
    public Boolean IsArchived { get; set; }

    public String PartitionKey { get; set; }
    public String RowKey { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public override String ToString() => $"{PartitionKey}/{Type}@{LatestVersion}";
}