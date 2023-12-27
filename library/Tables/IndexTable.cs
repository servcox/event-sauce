using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class IndexTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Task<Response> CreateOrUpdate(IndexRecord record, CancellationToken cancellationToken = default) =>
        table.UpsertEntityAsync(record, TableUpdateMode.Replace, cancellationToken);
}