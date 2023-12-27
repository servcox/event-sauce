using System.Text;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class ProjectionTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Pageable<ProjectionRecord> List(String projectionId, IDictionary<String, String> query)
    {
        var filter = new StringBuilder($"PartitionKey eq '{projectionId}'");
        foreach (var q in query)
        {
            var qValue = q.Value.Replace("'", "''");
            filter.Append($" and {q.Key} eq '{qValue}'");
        }

        return table.Query<ProjectionRecord>(filter.ToString());
    }


    public async Task Create(TableEntity record, CancellationToken cancellationToken) =>
        await table.AddEntityAsync(record, cancellationToken).ConfigureAwait(false);

    public async Task<TableEntity> ReadOrNew(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<TableEntity>(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null)
            return new()
            {
                PartitionKey = projectionId,
                RowKey = streamId,
            };
        return streamRecordWrapper.Value;
    }

    public Task<Response> Update(TableEntity record, CancellationToken cancellationToken = default) =>
        table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);

    public Task<Response> CreateOrUpdate(TableEntity record, CancellationToken cancellationToken = default) =>
        table.UpsertEntityAsync(record, TableUpdateMode.Replace, cancellationToken);

    public async Task Delete(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        try
        {
            await table.DeleteEntityAsync(projectionId, streamId, cancellationToken: cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityNotFound")
        {
            throw new NotFoundException();
        }
    }
    
    public async Task TryDelete(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        try
        {
            await table.DeleteEntityAsync(projectionId, streamId, cancellationToken: cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityNotFound")
        {
        }
    }
}