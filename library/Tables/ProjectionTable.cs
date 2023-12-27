using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class ProjectionTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public async Task Create(ProjectionRecord record, CancellationToken cancellationToken) =>
        await table.AddEntityAsync(record, cancellationToken).ConfigureAwait(false);

    public async Task<ProjectionRecord?> TryRead(String projectionId, String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<ProjectionRecord>(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) return null;
        return streamRecordWrapper.Value;
    }

    public Task<Response> Update(ProjectionRecord record, CancellationToken cancellationToken = default) =>
        table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
    
    public Task<Response> CreateOrUpdate(ProjectionRecord record, CancellationToken cancellationToken = default) =>
        table.UpsertEntityAsync(record, TableUpdateMode.Replace, cancellationToken);
}