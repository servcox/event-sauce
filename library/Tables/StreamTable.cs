using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class StreamTable(TableClient table)
{
    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Pageable<StreamRecord> List(String? streamType = default, DateTimeOffset? updatedSince = default, Boolean includeArchived = false) => table
        .Query<StreamRecord>(stream =>
            (streamType == null || stream.Type.Equals(streamType.ToUpperInvariant(), StringComparison.OrdinalIgnoreCase)) &&
            (!updatedSince.HasValue || stream.Timestamp >= updatedSince) &&
            (!stream.IsArchived || includeArchived)
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

    public Task<Response> Update(StreamRecord record, CancellationToken cancellationToken = default)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        return table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
    }
}