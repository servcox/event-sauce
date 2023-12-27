using System.Text.Json;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tables;
using Stream = ServcoX.EventSauce.Models.Stream;

namespace ServcoX.EventSauce;

public sealed class EventStore
{
    private const Int32 MaxEventsInWrite = 100; // Azure limit
    private readonly StreamTable _streamTable;
    private readonly EventTable _eventTable;
    private readonly ProjectionTable _projectionTable;
    private readonly IndexTable _indexTable;
    private readonly EventTypeResolver _eventTypeResolver = new();
    private readonly BaseConfiguration _configuration;

    public EventStore(String connectionString, Action<BaseConfiguration>? configure = null)
    {
        if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullOrDefaultException(nameof(connectionString));
        _configuration = new();
        configure?.Invoke(_configuration);

        _streamTable = new(new(connectionString, _configuration.StreamTableName));
        _eventTable = new(new(connectionString, _configuration.EventTableName));
        _projectionTable = new(new(connectionString, _configuration.ProjectionTableName));
        _indexTable = new(new(connectionString, _configuration.IndexTableName));

        if (_configuration.ShouldCreateTableIfMissing)
        {
            _streamTable.CreateUnderlyingIfNotExist();
            _eventTable.CreateUnderlyingIfNotExist();
            _projectionTable.CreateUnderlyingIfNotExist();
            _indexTable.CreateUnderlyingIfNotExist();
        }
    }

    /// <remarks>
    /// This operation is expensive, requiring a scan of all streams.
    /// </remarks>
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

    public async Task ArchiveStream(String streamId, CancellationToken cancellationToken = default)
    {
        if (String.IsNullOrEmpty(streamId)) throw new ArgumentNullOrDefaultException(nameof(streamId));

        var record = await _streamTable.Read(streamId, cancellationToken).ConfigureAwait(false);
        record.IsArchived = true;
        await _streamTable.Update(record, cancellationToken).ConfigureAwait(false);
    }

    public Task WriteStream(String streamId, IEventBody body, String createdBy, CancellationToken cancellationToken = default) =>
        WriteStream(streamId, new[] { body }, createdBy, cancellationToken);

    public Task WriteStream(String streamId, IEnumerable<IEventBody> body, String createdBy, CancellationToken cancellationToken = default) =>
        WriteStream(streamId, body.ToArray(), createdBy, cancellationToken);

    public async Task WriteStream(String streamId, IEventBody[] bodies, String createdBy, CancellationToken cancellationToken = default)
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
        });

        await _eventTable.CreateMany(eventRecords, cancellationToken).ConfigureAwait(false); // Must be first to avoid concurrency issues
        await _streamTable.Update(streamRecord, cancellationToken).ConfigureAwait(false);
    }

    public IEnumerable<Event> ReadStream(String streamId, UInt64 minVersion = 0)
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

    public async Task<TProjection> ReadProjection<TProjection>(String streamId, CancellationToken cancellationToken = default) where TProjection : new()
    {
        var type = typeof(TProjection);
        if (!_configuration.Projections.TryGetValue(type, out var builder)) throw new NotFoundException($"No projection for '{type.FullName}' defined");

        var record = await _projectionTable.TryRead(builder.Id, streamId, cancellationToken).ConfigureAwait(false) ?? new()
        {
            ProjectionId = builder.Id,
            StreamId = streamId,
        };
        var isNewProjection = String.IsNullOrEmpty(record.Body);
        var projection = isNewProjection ? new() : JsonSerializer.Deserialize<TProjection>(record.Body, _configuration.SerializationOptions) ?? throw new NeverNullException();
        var nextVersion = isNewProjection ? 0 : record.Version + 1;
        var events = ReadStream(streamId, nextVersion).ToList();

        if (events.Count != 0)
        {
            ApplyEvents(projection, events, builder);
            record.Body = JsonSerializer.Serialize(projection, _configuration.SerializationOptions);
            record.Version = events.Last().Version;

            if (isNewProjection)
            {
                await _projectionTable.Create(record, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _projectionTable.Update(record, cancellationToken).ConfigureAwait(false);
            }
        }

        return projection;
    }


    // TODO
    // public Task<TProjection> ListProjections<TProjection>() where TProjection : new() =>
    //     ListProjections<TProjection>(new Dictionary<String, String>());
    //
    // public Task<TProjection> ListProjections<TProjection>(String key, String value) where TProjection : new() =>
    //     ListProjections<TProjection>(new Dictionary<String, String> { [key] = value });
    //
    // public Task<TProjection> ListProjections<TProjection>(IDictionary<String, String> query) where TProjection : new()
    // {
    //     throw new NotImplementedException();
    // }

    // TODO:
    //  * Write index
    //  * Query on index
    //  * Background refresh index
    
    // ProjectionId|Field|Value, StreamId

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
}