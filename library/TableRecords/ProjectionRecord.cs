using System.Runtime.Serialization;
using Azure;
using Azure.Data.Tables;

// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.TableRecords;

public sealed class ProjectionRecord : ITableEntity
{
    [IgnoreDataMember]
    public String ProjectionId
    {
        get => PartitionKey;
        set => PartitionKey = value;
    }

    [IgnoreDataMember]
    public String StreamId
    {
        get => RowKey;
        set => RowKey = value;
    }

    public String Body { get; set; } = String.Empty;
    
    public UInt64 Version { get; set; }

    /// <remarks>
    /// Use `StreamId` instead.
    /// </remarks>
    public String PartitionKey { get; set; } = String.Empty;

    /// <summary>
    /// Use `Version` instead.
    /// </summary>
    public String RowKey { get; set; } = String.Empty;

    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public override String ToString() => $"{ProjectionId}/{StreamId}@{Version}";
}