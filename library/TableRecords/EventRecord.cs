using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.Utilities;

namespace ServcoX.EventSauce.TableRecords;

public sealed class EventRecord : ITableEntity
{
    public EventRecord()
    {
        PartitionKey = String.Empty;
        RowKey = String.Empty;
        Type = String.Empty;
        Body = String.Empty;
        CreatedBy = String.Empty;
    }
    
    public EventRecord(String streamId, UInt64 version, String type, String body, String createdBy)
    {
        PartitionKey = streamId;
        RowKey = RowKeyUtils.EncodeVersion(version);
        Type = type;
        Body = body;
        CreatedBy = createdBy;
    }

    public String Type { get; set; }
    public String Body { get; set; }
    public String CreatedBy { get; set; }

    public String PartitionKey { get; set; }
    public String RowKey { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }
    
    public override String ToString() => $"{PartitionKey}/{Type}@{RowKey}";
}