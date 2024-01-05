namespace ServcoX.EventSauce;

public class ProjectionStore
{
    public ProjectionStore(EventStore store, UInt64 version)
    {
        // Config:
        // * Projection definition
        //     * Index fields
        //     * How often to update cache
        throw new NotImplementedException();
    }

    public async Task<TProjection?> TryRead<TProjection>(String streamId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<TProjection> Read<TProjection>(String streamId, CancellationToken cancellationToken) => await TryRead<TProjection>(streamId, cancellationToken) ?? throw new NotFoundException();

    // TODO: query
}