using System.Runtime.Serialization;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.Extensions;

// ReSharper disable UnusedMember.Global
// ReSharper disable MemberCanBePrivate.Global

namespace ServcoX.EventSauce.TableRecords;

public sealed class IndexRecord : ITableEntity
{
    private const Char Separator = '/';

    [IgnoreDataMember]
    public String ProjectionId
    {
        get
        {
            var end = PartitionKey.IndexOf(Separator);
            if (end < 0) return String.Empty;
            return PartitionKey.Substring(0, end);
        }
        set => PartitionKey = $"{value}{Separator}{Field}{Separator}{Value}";
    }

    [IgnoreDataMember]
    public String Field
    {
        get
        {
            var start = PartitionKey.IndexOf(Separator);
            var end = PartitionKey.IndexOfNth(Separator, 2);
            if (start < 0 || end < 0) return String.Empty;
            return PartitionKey.Substring(start + 1, end - start - 1);
        }
        set
        {
            if (value.Contains(Separator)) throw new ArgumentOutOfRangeException(nameof(value), $"Cannot contain '{Separator}'");
            PartitionKey = $"{ProjectionId}{Separator}{value}{Separator}{Value}";
        }
    }

    [IgnoreDataMember]
    public String Value
    {
        get
        {
            var start = PartitionKey.IndexOfNth(Separator, 2);
            if (start < 0) return String.Empty;
            return PartitionKey.Substring(start + 1);
        }
        set => PartitionKey = $"{ProjectionId}{Separator}{Field}{Separator}{value}";
    }

    [IgnoreDataMember]
    public String StreamId
    {
        get => RowKey;
        set => RowKey = value;
    }

    /// <remarks>
    /// Use `ProjectionId`, `Field` and `Value instead.
    /// </remarks>
    public String PartitionKey { get; set; } = String.Empty;

    /// <summary>
    /// Use `StreamId` instead.
    /// </summary>
    public String RowKey { get; set; } = String.Empty;

    public DateTimeOffset? Timestamp { get; set; }
    public ETag ETag { get; set; }

    public override String ToString() => $"{ProjectionId}/{Field}={Value}@{StreamId}";
}