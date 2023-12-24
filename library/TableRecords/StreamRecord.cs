using Azure;
using Azure.Data.Tables;

// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.TableRecords;

public sealed class StreamRecord : ITableEntity
{
    public StreamRecord()
    {
        StreamId = String.Empty;
        Type = String.Empty;
    }

    public StreamRecord(String streamId, String type, UInt64 latestVersion, Boolean isArchived)
    {
        StreamId = streamId;
        Type = type; // TODO!!
        LatestVersion = latestVersion;
        IsArchived = isArchived;
    }

    public String StreamId
    {
        get => PartitionKey;
        init
        {
            PartitionKey = value;
            RowKey = value;
        }
    }

    public String Type { get; set; }
    public UInt64 LatestVersion { get; set; }
    public Boolean IsArchived { get; set; }

    /// <remarks>
    /// Use `StreamId` instead.
    /// </remarks>
    public String PartitionKey { get; set; } = String.Empty;

    /// <remarks>
    /// Use `StreamId` instead.
    /// </remarks>
    public String RowKey { get; set; } = String.Empty;

    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public override String ToString() => $"{Type}/{StreamId}@{LatestVersion}";
}