namespace ServcoX.EventSauce;

public interface IEventStore
{
    /// <summary>
    /// Get a list of all streams of a given type.
    /// </summary>
    /// <remarks>
    /// This is an expensive operation as it requires scanning all streams.
    /// </remarks>
    IEnumerable<Stream> ListStreams(String streamType);

    /// <summary>
    /// Create a new stream of a given type.
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task CreateStream(String streamId, String streamType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Create a new stream of a given type if it does not already exist.
    /// </summary>
    Task CreateStreamIfNotExist(String streamId, String streamType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Read the metadata of a given stream. 
    /// </summary>
    /// <remarks>
    /// NOTE: This does not include events. Use `ReadEvents` to retrieve events in a stream.
    /// </remarks>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task<Stream> ReadStream(String streamId, CancellationToken cancellationToken);

    /// <summary>
    /// Archive a stream, removing it from stream lists and projections.
    /// </summary>
    /// <remarks>
    /// This is akin to deletion, however with event sourcing data is immutably stored and not removed.
    /// </remarks>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task ArchiveStream(String streamId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Unarchive a stream, returning it to lists and projections
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task UnarchiveStream(String streamId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Write a single event to a pre-existing event stream.
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task WriteEvents(String streamId, IEventBody body, String createdBy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Write multiple events to a pre-existing event stream.
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task WriteEvents(String streamId, IEnumerable<IEventBody> body, String createdBy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Write multiple events to a pre-existing event stream.
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    Task WriteEvents(String streamId, IEventBody[] bodies, String createdBy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Read events from a given event stream, and from a given minimum version
    /// </summary>
    /// <exception cref="ArgumentNullOrDefaultException"></exception>
    IEnumerable<Event> ReadEvents(String streamId, UInt64 minVersion = 0);

    /// <summary>
    /// Read a projection, aggregating any new events into the projection if required.
    /// </summary>
    /// <exception cref="NotFoundException"></exception>
    /// <exception cref="StreamArchivedException"></exception>
    Task<TProjection> ReadProjection<TProjection>(String streamId, CancellationToken cancellationToken = default) where TProjection : new();
    
    /// <summary>
    /// Read a projection, aggregating any new events into the projection if required.
    /// </summary>
    Task<TProjection?> TryReadProjection<TProjection>(String streamId, CancellationToken cancellationToken = default) where TProjection : new();

    /// <summary>
    /// Return all projections of a given type.
    /// </summary>
    IEnumerable<TProjection> ListProjections<TProjection>() where TProjection : new();

    /// <summary>
    /// Return all projections of a given type matching a single filter.
    /// </summary>
    IEnumerable<TProjection> ListProjections<TProjection>(String key, String value) where TProjection : new();

    /// <summary>
    /// Return all projections of a given type matching a multiple filters.
    /// </summary>
    IEnumerable<TProjection> ListProjections<TProjection>(IDictionary<String, String> query) where TProjection : new();

    /// <summary>
    /// Refresh all projections that have changed, optionally since a given time.
    /// </summary>
    Task<DateTimeOffset> RefreshAllProjections(DateTimeOffset updatedSince = default, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Refresh projections (and their indexes) for a given stream.
    /// </summary>
    Task RefreshProjections(String streamId, CancellationToken cancellationToken = default);
}