using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class StreamTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Pageable<StreamRecord> List(String streamType) => table
        .Query<StreamRecord>(stream =>
            stream.Type.Equals(streamType.ToUpperInvariant(), StringComparison.OrdinalIgnoreCase) &&
            !stream.IsArchived
        );

    public async Task Create(StreamRecord record, CancellationToken cancellationToken)
    {
        try
        {
            await table.AddEntityAsync(record, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
        {
            throw new AlreadyExistsException();
        }
    }

    public async Task<StreamRecord> Read(String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<StreamRecord>(streamId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) throw new NotFoundException();
        return streamRecordWrapper.Value;
    }

    public Task<Response> Update(StreamRecord record, CancellationToken cancellationToken = default) =>
        table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
}