using System.Diagnostics;
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tables;
using Timer = System.Timers.Timer;

namespace ServcoX.EventSauce;

public sealed class EventStore : IDisposable, IEventStore
{
    private const Int32 MaxEventsInWrite = 100; // Azure limit
    private readonly StreamTable _streamTable;
    private readonly EventTable _eventTable;
    private readonly ProjectionTable _projectionTable;
    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly BaseConfiguration _configuration;
    private Timer? _projectionRefreshTimer;

    public EventStore(String connectionString, Action<BaseConfiguration>? configure = null)
    {
        if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullOrDefaultException(nameof(connectionString));
        _configuration = new();
        configure?.Invoke(_configuration);

        ThrowExceptionOnBadIndexName();

        _streamTable = new(new(connectionString, _configuration.StreamTableName));
        _eventTable = new(new(connectionString, _configuration.EventTableName));
        _projectionTable = new(new(connectionString, _configuration.ProjectionTableName));

        if (_configuration.ShouldRefreshProjectionsOnStartup) _ = RefreshAllProjections();
        
        if (_configuration.ShouldCreateTableIfMissing)
        {
            _streamTable.CreateUnderlyingIfNotExist();
            _eventTable.CreateUnderlyingIfNotExist();
            _projectionTable.CreateUnderlyingIfNotExist();
        }

        SetupProjectionRefreshTimer();
    }

    public IEnumerable<Stream> ListStreams(String streamType)
    {
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        return _streamTable.List(streamType).Select(Stream.CreateFrom);
    }

    public async Task CreateStream(String streamId, String streamType, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));
        if (String.IsNullOrEmpty(streamType)) throw new ArgumentNullOrDefaultException(nameof(streamType));

        await _streamTable.Create(new()
        {
            StreamId = streamId,
            Type = streamType.ToUpperInvariant(),
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task CreateStreamIfNotExist(String streamId, String streamType, CancellationToken cancellationToken = default)
    {
        try
        {
            await CreateStream(streamId, streamType, cancellationToken).ConfigureAwait(false);
        }
        catch (AlreadyExistsException)
        {
        }
    }

    public async Task<Stream> ReadStream(String streamId, CancellationToken cancellationToken)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var record = await _streamTable.Read(streamId, cancellationToken).ConfigureAwait(false);
        return Stream.CreateFrom(record);
    }

    public async Task ArchiveStream(String streamId, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var record = await _streamTable.Read(streamId, cancellationToken).ConfigureAwait(false);
        record.IsArchived = true;
        await _streamTable.Update(record, cancellationToken).ConfigureAwait(false);
    }

    public async Task UnarchiveStream(String streamId, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var record = await _streamTable.Read(streamId, cancellationToken).ConfigureAwait(false);
        record.IsArchived = true;
        await _streamTable.Update(record, cancellationToken).ConfigureAwait(false);
    }

    public Task WriteEvents(String streamId, IEventBody body, String createdBy, CancellationToken cancellationToken = default) =>
        WriteEvents(streamId, new[] { body }, createdBy, cancellationToken);

    public Task WriteEvents(String streamId, IEnumerable<IEventBody> body, String createdBy, CancellationToken cancellationToken = default) =>
        WriteEvents(streamId, body.ToArray(), createdBy, cancellationToken);

    public async Task WriteEvents(String streamId, IEventBody[] bodies, String createdBy, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));
        if (bodies is null) throw new ArgumentNullException(nameof(bodies));
        if (bodies.Length == 0) return; // Azure Table Storage throws an exception if you ask it to do nothing
        if (bodies.Length >= MaxEventsInWrite) throw new InvalidOperationException("At most 100 events can be queued in a writer before a commit is required");

        var streamRecord = await _streamTable.Read(streamId, cancellationToken).ConfigureAwait(false);

        var eventRecords = bodies.Select(body =>
        {
            var type = _eventTypeResolver.Encode(body.GetType());
            var bodySerialized = JsonSerializer.Serialize((Object)body, _configuration.SerializationOptions);
            var version = ++streamRecord.LatestVersion;

            return new EventRecord
            {
                StreamId = streamId,
                Version = version,
                Type = type,
                Body = bodySerialized,
                CreatedBy = createdBy,
            };
        }).ToList();

        await _streamTable.Update(streamRecord, cancellationToken).ConfigureAwait(false);
        await _eventTable.CreateMany(eventRecords, cancellationToken).ConfigureAwait(false); // Must be last so that an interrupted write doesn't break the MaxVersion header
        if (_configuration.ShouldRefreshProjectionsAfterWriting) await TryRefreshProjections(streamRecord, cancellationToken).ConfigureAwait(false);
    }

    public IEnumerable<Event> ReadEvents(String streamId, UInt64 minVersion = 0)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        return _eventTable
            .List(streamId, minVersion)
            .Select(eventRecord =>
            {
                var type = _eventTypeResolver.TryDecode(eventRecord.Type);
                var body = type is null ? null : (IEventBody)JsonSerializer.Deserialize(eventRecord.Body, type, _configuration.SerializationOptions)! ?? throw new NeverNullException();
                return Event.CreateFrom(eventRecord, body);
            });
    }

    public async Task<TProjection?> TryReadProjection<TProjection>(String streamId, CancellationToken cancellationToken = default) where TProjection : new()
    {
        if (_configuration.ShouldRefreshProjectionsBeforeReading)
        {
            var streamRecord = await _streamTable.TryRead(streamId, cancellationToken).ConfigureAwait(false);
            if (streamRecord is null) throw new NotFoundException();
            await TryRefreshProjections(new List<StreamRecord> { streamRecord }, cancellationToken).ConfigureAwait(false);
        }

        var projectionType = typeof(TProjection);
        if (!_configuration.Projections.TryGetValue(projectionType, out var builder)) throw new NotFoundException($"No projection for '{projectionType.FullName}' defined");

        var record = await _projectionTable.TryRead(builder.Id, streamId, cancellationToken).ConfigureAwait(false);
        if (record is null) return default;
        var projection = JsonSerializer.Deserialize<TProjection>(record.Body, _configuration.SerializationOptions) ?? throw new NeverNullException();

        return projection;
    }

    public async Task<TProjection> ReadProjection<TProjection>(String streamId, CancellationToken cancellationToken = default) where TProjection : new()
    {
        var projection = await TryReadProjection<TProjection>(streamId, cancellationToken).ConfigureAwait(false);
        if (projection is null) throw new NotFoundException();
        return projection;
    }

    public IEnumerable<TProjection> ListProjections<TProjection>() where TProjection : new() =>
        ListProjections<TProjection>(new Dictionary<String, String>());

    public IEnumerable<TProjection> ListProjections<TProjection>(String key, String value) where TProjection : new() =>
        ListProjections<TProjection>(new Dictionary<String, String> { [key] = value });

    public IEnumerable<TProjection> ListProjections<TProjection>(IDictionary<String, String> query) where TProjection : new()
    {
        var type = typeof(TProjection);
        if (!_configuration.Projections.TryGetValue(type, out var builder)) throw new NotFoundException($"No projection for '{type.FullName}' defined");

        var records = _projectionTable.List(builder.Id, query);

        return records.Select(record => JsonSerializer.Deserialize<TProjection>(record.Body, _configuration.SerializationOptions)!);
    }

    public async Task<DateTimeOffset> RefreshAllProjections(DateTimeOffset updatedSince = default, CancellationToken cancellationToken = default)
    {
        Trace.WriteLine("RefreshAll started");

        var streamTypesWithProjections = _configuration.Projections
            .Select(p => p.Value.StreamType)
            .Distinct()
            .ToList();

        var maxTimestamp = new DateTimeOffset();
        foreach (var streamType in streamTypesWithProjections)
        {
            foreach (var page in _streamTable.List(streamType, updatedSince: updatedSince).AsPages(pageSizeHint: 1000))
            {
                var stopwatch = Stopwatch.StartNew();
                foreach (var stream in page.Values)
                {
                    await TryRefreshProjections(stream, cancellationToken).ConfigureAwait(false);
                    if (stream.Timestamp > maxTimestamp && stream.Timestamp.HasValue) maxTimestamp = stream.Timestamp.Value;
                }

                Trace.WriteLine($"Refreshed projections of {page.Values.Count} '{streamType}' streams in {stopwatch.ElapsedMilliseconds}ms");
            }
        }

        Trace.WriteLine($"RefreshAll completed, with max timestamp of {maxTimestamp}");

        return maxTimestamp;
    }

    public async Task RefreshProjections(String streamId, CancellationToken cancellationToken = default)
    {
        var streamRecord = await _streamTable.TryRead(streamId, cancellationToken).ConfigureAwait(false);
        if (streamRecord is null) throw new NotFoundException();
        await TryRefreshProjections(new List<StreamRecord> { streamRecord }, cancellationToken).ConfigureAwait(false);
    }

    private Task TryRefreshProjections(StreamRecord streamRecord, CancellationToken cancellationToken = default) =>
        TryRefreshProjections(new List<StreamRecord> { streamRecord }, cancellationToken);

    private async Task TryRefreshProjections(IList<StreamRecord> streamRecords, CancellationToken cancellationToken = default)
    {
        foreach (var streamRecord in streamRecords)
        {
            var streamId = streamRecord.StreamId;
            var streamType = streamRecord.Type;

            foreach (var p in _configuration.Projections)
            {
                var projectionType = p.Key;
                var projectionId = p.Value.Id;
                var builder = p.Value;

                if (builder.StreamType != streamType) continue;

                if (streamRecord.IsArchived)
                {
                    await _projectionTable.TryDelete(projectionId, streamRecord.StreamId, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var projectionRecord = await _projectionTable.ReadOrNewGeneric(builder.Id, streamId, cancellationToken).ConfigureAwait(false);
                var projectionBody = projectionRecord.GetString(nameof(ProjectionRecord.Body));
                var isNewProjection = String.IsNullOrEmpty(projectionBody);
                var projection = (isNewProjection ? Activator.CreateInstance(projectionType) : JsonSerializer.Deserialize(projectionBody, projectionType, _configuration.SerializationOptions)) ??
                                 throw new NeverNullException();
                var nextVersion = isNewProjection ? 0 : (UInt64)(projectionRecord.GetInt64(nameof(ProjectionRecord.Version)) ?? throw new NeverNullException()) + 1;
                // TODO: Possible to only read events once, even when involved in multiple projections
                var events = ReadEvents(streamId, nextVersion).ToList();

                if (events.Count != 0)
                {
                    ApplyEvents(streamId, projection, events, builder, isNewProjection);
                    projectionRecord[nameof(ProjectionRecord.Body)] = JsonSerializer.Serialize(projection, _configuration.SerializationOptions);
                    projectionRecord[nameof(ProjectionRecord.Version)] = events.Last().Version;
                    UpdateIndexes(builder, projection, projectionRecord);

                    if (isNewProjection)
                    {
                        try
                        {
                            await _projectionTable.CreateGeneric(projectionRecord, cancellationToken).ConfigureAwait(false);
                        }
                        catch (RequestFailedException ex) when (ex.ErrorCode == "EntityAlreadyExists")
                        {
                            // Another projection refresh beat us - nothing to do
                        }
                    }
                    else
                    {
                        try
                        {
                            await _projectionTable.UpdateGeneric(projectionRecord, cancellationToken).ConfigureAwait(false);
                        }
                        catch (RequestFailedException ex) when (ex.ErrorCode == "UpdateConditionNotSatisfied")
                        {
                            // Another projection refresh beat us - nothing to do
                        }
                    }
                }
            }
        }
    }

    private static void UpdateIndexes(IProjectionBuilder builder, Object projection, TableEntity record)
    {
        foreach (var index in builder.Indexes)
        {
            var field = index.Key;
            var method = index.Value
                .GetType()
                .GetMethod(nameof(Func<String>.Invoke)) ?? throw new NeverNullException(); // TODO: Better performance by precomputing this?

            var value = (String)method.Invoke(index.Value, new[] { projection })!;

            record[field] = value;
        }
    }

    private void ApplyEvents(String streamId, Object projection, List<Event> events, IProjectionBuilder builder, Boolean isNew)
    {
        if (isNew)
        {
            // foreach (var handler in builder.CreationHandlers) ((Action<TProjection, String>)handler)(projection, streamId);
            foreach (var handler in builder.CreationHandlers)
            {
                var method = handler
                    .GetType()
                    .GetMethod(nameof(Action.Invoke)) ?? throw new NeverNullException(); // TODO: Better performance by precomputing this?
                method.Invoke(handler, new[] { projection, streamId });
            }
        }

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
                        method.Invoke(handler, new[] { projection, evt.Body, evt });
                    }
                }
            }

            if (!specificHandlerFound)
            {
                foreach (var handler in builder.FallbackHandlers)
                {
                    var method = handler
                        .GetType()
                        .GetMethod(nameof(Action.Invoke)) ?? throw new NeverNullException(); // TODO: Better performance by precomputing this?
                    method.Invoke(handler, new[] { projection, evt });
                }
            }


            foreach (var handler in builder.PromiscuousHandlers)
            {
                var method = handler
                    .GetType()
                    .GetMethod(nameof(Action.Invoke)) ?? throw new NeverNullException(); // TODO: Better performance by precomputing this?
                method.Invoke(handler, new[] { projection, evt });
            }
        }
    }

    private void ThrowExceptionOnBadIndexName()
    {
        var prohibitedIndexNames = typeof(ProjectionRecord).GetMembers().Select(field => field.Name.ToUpperInvariant()).ToList();
        foreach (var projection in _configuration.Projections)
        {
            foreach (var index in projection.Value.Indexes)
            {
                if (prohibitedIndexNames.Contains(index.Key.ToUpperInvariant()))
                    throw new InvalidIndexNameException($"Projection '{projection.Key}' cannot have index using reserved name '{index.Key}'");
                if (index.Key.Length > 255) throw new InvalidIndexNameException($"Projection '{projection.Key}' has index with name longer than 255 characters");
            }
        }
    }

    private void SetupProjectionRefreshTimer()
    {
        if (_configuration.ProjectionRefreshInterval.HasValue)
        {
            _projectionRefreshTimer = new(_configuration.ProjectionRefreshInterval.Value.TotalMilliseconds);
            var lastRun = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            _projectionRefreshTimer.Elapsed += async (_, _) =>
            {
                lastRun = await RefreshAllProjections(lastRun).ConfigureAwait(false);
                _projectionRefreshTimer.Start();
            };
            _projectionRefreshTimer.AutoReset = false;
            _projectionRefreshTimer.Start();
        }
    }

    public void Dispose()
    {
        _projectionRefreshTimer?.Dispose();
    }
}