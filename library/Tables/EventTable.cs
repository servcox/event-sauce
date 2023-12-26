using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Utilities;

namespace ServcoX.EventSauce.Tables;

public sealed class EventTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Pageable<EventRecord> List(String streamId, UInt64 minVersion) => table
        .Query<EventRecord>(record =>
            record.PartitionKey == streamId &&
            String.Compare(record.RowKey, RowKeyUtilities.EncodeVersion(minVersion), StringComparison.Ordinal) >= 0); // RowKeys are string, so we need to encode the int as a string to compare

    public async Task CreateMany(IEnumerable<EventRecord> records, CancellationToken cancellationToken)
    {
        try
        {
            // See https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/tables/Azure.Data.Tables/samples/Sample6TransactionalBatch.md
            var batch = records.Select(record => new TableTransactionAction(TableTransactionActionType.Add, record));
            await table.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        catch (TableTransactionFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
        {
            throw new OptimisticWriteInterruptedException("Write to this stream interrupted by a preceding write. Retry the operation.");
        }
    }
}