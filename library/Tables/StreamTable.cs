using System.Globalization;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tables;

public sealed class StreamTable(TableClient table)
{
    private readonly DateTime _minTimestamp = new(2000, 1, 1);

    public void CreateUnderlyingIfNotExist() =>
        table.CreateIfNotExists();

    public Pageable<StreamRecord> List(String streamType, DateTimeOffset updatedSince = default)
    {
        if (streamType is null) throw new ArgumentNullException(nameof(streamType));
        if (updatedSince < _minTimestamp) updatedSince = _minTimestamp; // Azure table storage doesn't like heaps-old timestamps

        // Queries using lambda syntax get complicated due to OData's limitations
        var query = $"Type eq '{streamType.ToUpperInvariant().Replace("'", "''")}' and Timestamp ge DateTime'{updatedSince.ToString("o", CultureInfo.InvariantCulture)}'";
        return table.Query<StreamRecord>(query);
    }

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

    public async Task<StreamRecord?> TryRead(String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecordWrapper = await table.GetEntityIfExistsAsync<StreamRecord>(streamId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) return null;
        return streamRecordWrapper.Value;
    }

    public Task<Response> Update(StreamRecord record, CancellationToken cancellationToken = default)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        try
        {
            return table.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "UpdateConditionNotSatisfied")
        {
            throw new OptimisticWriteInterruptedException("Write to this stream interrupted by a preceding write. Retry the operation.");
        }
    }
}