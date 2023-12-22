using System.Text.Json;
using System.Text.Json.Serialization;
using Azure;
using Azure.Data.Tables;

namespace ServcoX.EventSauce.EventStores;

public sealed class EventStore
{
    private const UInt64 ImplicitInitialVersion = 0;
    private const Int32 MaxEventsInWrite = 100; // Azure limit
    private readonly TableClient _streamTable;
    private readonly TableClient _eventTable;
    private readonly EventTypeResolver _eventTypeResolver = new();

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public EventStore(String connectionString, String? postfix = null)
    {
        if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullOrDefaultException(nameof(connectionString));

        _streamTable = new(connectionString, postfix is null ? "stream" : $"stream{postfix.ToUpperInvariant()}");
        _streamTable.CreateIfNotExists();
        _eventTable = new(connectionString, postfix is null ? "event" : $"event{postfix.ToUpperInvariant()}");
        _eventTable.CreateIfNotExists();
    }

    public IEnumerable<EventStream> ListStreams(String streamType)
    {
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        streamType = streamType.ToUpperInvariant();
        return _streamTable
            .Query<EventStreamRecord>(stream =>
                stream.Type.Equals(streamType, StringComparison.OrdinalIgnoreCase)
                && !stream.IsArchived)
            .Select(EventStream.CreateFrom);
    }
    
    public async Task CreateStream(String streamId, String streamType, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        streamType = streamType.ToUpperInvariant();
        await _streamTable.AddEntityAsync(new EventStreamRecord(streamId, streamType, ImplicitInitialVersion, false), cancellationToken).ConfigureAwait(false);
    }

    public async Task CreateStreamIfNotExist(String streamId, String streamType, CancellationToken cancellationToken)
    {
        try
        {
            await CreateStream(streamId, streamType, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
        {
        }
    }

    public async Task ArchiveStream(String streamId, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var streamRecord = await _streamTable.GetEntityAsync<EventStreamRecord>(streamId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false) ?? throw new NeverNullException();
        streamRecord.Value.IsArchived = true;
        await _streamTable.UpdateEntityAsync(streamRecord.Value, streamRecord.Value.ETag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
    }

    public Task WriteStream(String streamId, IEventBody payload, String createdBy, CancellationToken cancellationToken) =>
        WriteStream(streamId, new[] { payload }, createdBy, cancellationToken);

    public Task WriteStream(String streamId, IEnumerable<IEventBody> payloads, String createdBy, CancellationToken cancellationToken) =>
        WriteStream(streamId, payloads.ToArray(), createdBy, cancellationToken);

    public async Task WriteStream(String streamId, IEventBody[] payloads, String createdBy, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));
        ArgumentNullException.ThrowIfNull(payloads);
        if (payloads.Length == 0) return;
        if (payloads.Length >= MaxEventsInWrite) throw new InvalidOperationException("At most 100 events can be queued in a writer before a commit is required");

        var streamRecord = await GetStreamRecord(streamId, cancellationToken).ConfigureAwait(false);

        var batch = payloads.Select(payload =>
        {
            var type = _eventTypeResolver.Encode(payload.GetType());
            var body = JsonSerializer.Serialize((Object)payload, SerializerOptions);
            var version = ++streamRecord.LatestVersion;

            return new TableTransactionAction(TableTransactionActionType.Add, new EventRecord(
                streamId,
                version,
                type,
                body,
                createdBy
            ));
        });

        try
        {
            // See https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/tables/Azure.Data.Tables/samples/Sample6TransactionalBatch.md
            await _eventTable.SubmitTransactionAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        catch (TableTransactionFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
        {
            throw new OptimisticWriteInterruptedException("Write to this stream interrupted by a preceding write. Retry the operation.");
        }

        await _streamTable.UpdateEntityAsync(streamRecord, streamRecord.ETag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
    }

    public IEnumerable<Event> ReadStream(String streamId, UInt64 minVersion = ImplicitInitialVersion)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        return _eventTable
            .Query<EventRecord>(record =>
                record.PartitionKey == streamId &&
                String.Compare(record.RowKey, RowKeyUtils.EncodeVersion(minVersion), StringComparison.Ordinal) >= 0) // RowKeys are string, so we need to encode the int as a string to compare
            .Select(eventRecord =>
            {
                var type = _eventTypeResolver.TryDecode(eventRecord.Type);
                var body = type is null ? null : (IEventBody)JsonSerializer.Deserialize(eventRecord.Body, type, SerializerOptions)! ?? throw new NeverNullException();
                return Event.CreateFrom(eventRecord, body);
            });
    }
    
    private async Task<EventStreamRecord> GetStreamRecord(String streamId, CancellationToken cancellationToken)
    {
        var streamRecordWrapper = await _streamTable.GetEntityIfExistsAsync<EventStreamRecord>(streamId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) throw new NotFoundException();
        return streamRecordWrapper.Value;
    }
    
    public void CreateProjection<TProjection>(String streamType, Func<IProjector<TProjection>> generateProjector)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<TProjection> QueryProjection<TProjection>(Func<TProjection, Boolean> query)
    {
        throw new NotImplementedException();
    }

    public TProjection FindProjection<TProjection>(String streamId)
    {
        throw new NotImplementedException();
    }
}