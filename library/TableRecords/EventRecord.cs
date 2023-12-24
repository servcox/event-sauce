using System.Globalization;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.Utilities;

// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.TableRecords;

public sealed class EventRecord : ITableEntity
{
    public EventRecord()
    {
        StreamId = String.Empty;
        Version = 0;
        Type = String.Empty;
        Body = String.Empty;
        CreatedBy = String.Empty;
    }

    public EventRecord(String streamId, UInt64 version, String type, String body, String createdBy)
    {
        StreamId = streamId;
        Version = version;
        Type = type;
        Body = body;
        CreatedBy = createdBy;
    }

    public String StreamId
    {
        get => PartitionKey;
        init => PartitionKey = value;
    }

    public UInt64 Version
    {
        get => UInt64.Parse(RowKey, CultureInfo.InvariantCulture);
        init => RowKey = RowKeyUtilities.EncodeVersion(value);
    }

    public String Type { get; set; }
    public String Body { get; set; }
    public String CreatedBy { get; set; }

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

    public override String ToString() => $"{StreamId}/{Type}@{Version}";
}