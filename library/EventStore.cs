using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Utilities;
using Stream = ServcoX.EventSauce.Models.Stream;

namespace ServcoX.EventSauce;

public sealed class EventStore
{
    private const Int32 MaxEventsInWrite = 100; // Azure limit
    private readonly TableClient _streamTable;
    private readonly TableClient _eventTable;
    private readonly TableClient _projectionTable;
    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly BaseConfiguration _configuration;

    public EventStore(String connectionString, Action<BaseConfiguration>? configure = null)
    {
        if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullOrDefaultException(nameof(connectionString));
        _configuration = new();
        configure?.Invoke(_configuration);

        _streamTable = new(connectionString, _configuration.StreamTableName);
        _eventTable = new(connectionString, _configuration.EventTableName);
        _projectionTable = new(connectionString, _configuration.ProjectionTableName);

        if (_configuration.ShouldCreateTableIfMissing)
        {
            _streamTable.CreateIfNotExists();
            _eventTable.CreateIfNotExists();
            _projectionTable.CreateIfNotExists();
        }
    }

    /// <remarks>
    /// This is an expensive operation, requiring a scan of all streams.
    /// </remarks>
    public IEnumerable<Stream> ListStreams(String streamType)
    {
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        streamType = streamType.ToUpperInvariant();
        return _streamTable
            .Query<StreamRecord>(stream =>
                stream.Type.Equals(streamType, StringComparison.OrdinalIgnoreCase) &&
                !stream.IsArchived
            )
            .Select(Stream.CreateFrom);
    }

    public async Task CreateStream(String streamId, String streamType, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        streamType = streamType.ToUpperInvariant();
        try
        {
            await _streamTable.AddEntityAsync(new StreamRecord
            {
                StreamId = streamId,
                Type = streamType,
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
        {
            throw new AlreadyExistsException();
        }
    }

    public async Task CreateStreamIfNotExist(String streamId, String streamType, CancellationToken cancellationToken)
    {
        try
        {
            await CreateStream(streamId, streamType, cancellationToken).ConfigureAwait(false);
        }
        catch (AlreadyExistsException)
        {
        }
    }

    public async Task ArchiveStream(String streamId, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var streamRecord = await GetStreamRecord(streamId, cancellationToken).ConfigureAwait(false);
        streamRecord.IsArchived = true;
        await UpdateStreamRecord(streamRecord, cancellationToken).ConfigureAwait(false);
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
            var body = JsonSerializer.Serialize((Object)payload, _configuration.SerializationOptions);
            var version = ++streamRecord.LatestVersion;

            return new TableTransactionAction(TableTransactionActionType.Add, new EventRecord
            {
                StreamId = streamId,
                Version = version,
                Type = type,
                Body = body,
                CreatedBy = createdBy,
            });
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

        await UpdateStreamRecord(streamRecord, cancellationToken).ConfigureAwait(false);
    }

    public IEnumerable<Event> ReadStream(String streamId, UInt64 minVersion = 0)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        return _eventTable
            .Query<EventRecord>(record =>
                record.PartitionKey == streamId &&
                String.Compare(record.RowKey, RowKeyUtilities.EncodeVersion(minVersion), StringComparison.Ordinal) >= 0) // RowKeys are string, so we need to encode the int as a string to compare
            .Select(eventRecord =>
            {
                var type = _eventTypeResolver.TryDecode(eventRecord.Type);
                var body = type is null ? null : (IEventBody)JsonSerializer.Deserialize(eventRecord.Body, type, _configuration.SerializationOptions)! ?? throw new NeverNullException();
                return Event.CreateFrom(eventRecord, body);
            });
    }

    public async Task<TProjection> ReadProjection<TProjection>(String streamId, CancellationToken cancellationToken) where TProjection : new()
    {
        var type = typeof(TProjection);
        if (!_configuration.Projections.TryGetValue(type, out var builder)) throw new NotFoundException($"No projection for '{type.FullName}' defined");

        var record = await TryGetProjectionRecord(builder.Id, streamId, cancellationToken).ConfigureAwait(false) ?? new()
        {
            ProjectionId = builder.Id,
            StreamId = streamId,
        };
        var isNewProjection = String.IsNullOrEmpty(record.Body);
        var projection = isNewProjection ? new(): JsonSerializer.Deserialize<TProjection>(record.Body, _configuration.SerializationOptions) ?? throw new NeverNullException();
        var nextVersion = isNewProjection ? 0 : record.Version + 1;
        var events = ReadStream(streamId, nextVersion).ToList();

        if (events.Count != 0)
        {
            ApplyEvents(projection, events, builder);
            record.Body = JsonSerializer.Serialize(projection, _configuration.SerializationOptions);
            record.Version = events.Last().Version;

            if (isNewProjection)
            {
                await _projectionTable.AddEntityAsync(record, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await UpdateProjectionRecord(record, cancellationToken).ConfigureAwait(false);
            }
        }


        // TODO: Test persistance

        return projection;
    }

    public Task<TProjection> ListProjections<TProjection>(String key, String value) where TProjection : new() =>
        ListProjections<TProjection>(new Dictionary<String, String> { [key] = value });

    public Task<TProjection> ListProjections<TProjection>(IDictionary<String, String> query) where TProjection : new()
    {
        throw new NotImplementedException();
    }

    private void ApplyEvents<TProjection>(TProjection projection, List<Event> events, IProjectionBuilder builder) where TProjection : new()
    {
        foreach (var evt in events)
        {
            var specificHandlerFound = false;
            var eventType = _eventTypeResolver.TryDecode(evt.Type);
            if (eventType is not null)
            {
                if (builder.EventHandlers.TryGetValue(eventType, out var handlers))
                {
                    specificHandlerFound = true;
                    foreach (var handler in handlers)
                    {
                        var method = handler
                            .GetType()
                            .GetMethod(nameof(Action.Invoke)) ?? throw new NeverNullException();
                        method.Invoke(handler, new Object?[] { projection, evt.Body, evt });
                    }
                }
            }

            if (!specificHandlerFound)
            {
                foreach (var handler in builder.FallbackHandlers) ((Action<TProjection, Event>)handler)(projection, evt);
            }

            foreach (var handler in builder.PromiscuousHandlers) ((Action<TProjection, Event>)handler)(projection, evt);
        }
    }

    private async Task<ProjectionRecord?> TryGetProjectionRecord(String projectionId, String streamId, CancellationToken cancellationToken)
    {
        var streamRecordWrapper = await _projectionTable.GetEntityIfExistsAsync<ProjectionRecord>(projectionId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) return null;
        return streamRecordWrapper.Value;
    }

    private Task<Response> UpdateProjectionRecord(ProjectionRecord record, CancellationToken cancellationToken) =>
        _projectionTable.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);

    private async Task<StreamRecord> GetStreamRecord(String streamId, CancellationToken cancellationToken)
    {
        var streamRecordWrapper = await _streamTable.GetEntityIfExistsAsync<StreamRecord>(streamId, streamId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!streamRecordWrapper.HasValue || streamRecordWrapper.Value is null) throw new NotFoundException();
        return streamRecordWrapper.Value;
    }

    private Task<Response> UpdateStreamRecord(StreamRecord record, CancellationToken cancellationToken) =>
        _streamTable.UpdateEntityAsync(record, record.ETag, TableUpdateMode.Replace, cancellationToken);
}