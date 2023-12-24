using Azure;
using Azure.Data.Tables;

// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.TableRecords;

public sealed class StreamRecord : ITableEntity
{
    public String StreamId
    {
        get => PartitionKey;
        init
        {
            PartitionKey = value;
            RowKey = value;
        }
    }

    public String Type { get; set; } = String.Empty;
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