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
        if (query is null) throw new ArgumentNullException(nameof(query));
        
        var filter = $"PartitionKey eq '{projectionId}'";
        foreach (var q in query)
        {
            var qValue = q.Value.Replace("'", "''");
            filter += $" and {q.Key} eq '{qValue}'";
        }

        return table.Query<ProjectionRecord>(filter);
    }


    public async Task CreateGeneric(TableEntity record, CancellationToken cancellationToken) =>
        await table.AddEntityAsync(record, cancellationToken).ConfigureAwait(false);

    public async Task<ProjectionRecord> Read(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<ProjectionRecord>(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) throw new NotFoundException();
        return streamRecordWrapper.Value;
    }
    
    public async Task<ProjectionRecord?> TryRead(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<ProjectionRecord>(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) return null;
        return streamRecordWrapper.Value;
    }
    
    public async Task<TableEntity> ReadOrNewGeneric(String projectionId, String streamId, CancellationToken cancellationToken = default)
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

    public Task<Response> UpdateGeneric(TableEntity record, CancellationToken cancellationToken = default)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        return table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
    }

    public async Task Delete(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        try
        {
            await table.DeleteEntityAsync(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
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
            await table.DeleteEntityAsync(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityNotFound")
        {
        }
    }
}