namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionStoreConfiguration
{
    public TimeSpan CacheUpdateInterval { get; private set; } = TimeSpan.FromMinutes(15);

    public ProjectionStoreConfiguration WriteRemoteCacheEvery(TimeSpan interval)
    {
        CacheUpdateInterval = interval;
        return this;
    }
    
    public TimeSpan? SyncInterval { get; private set; }

    public ProjectionStoreConfiguration SyncEvery(TimeSpan interval)
    {
        SyncInterval = interval;
        return this;
    }

    public Boolean SyncBeforeReadEnabled { get; private set; } = true;

    public ProjectionStoreConfiguration DoNotSyncBeforeReads()
    {
        SyncBeforeReadEnabled = false;
        return this;
    }

    private readonly Dictionary<Type, IProjectionConfiguration> _projections = new();
    public IReadOnlyDictionary<Type, IProjectionConfiguration> Projections => _projections;

    public ProjectionStoreConfiguration DefineProjection<TProjection>(Int64 version, Action<ProjectionConfiguration<TProjection>> build) where TProjection : new()
    {
        if (build is null) throw new ArgumentNullException(nameof(build));

        var type = typeof(TProjection);
        var builder = new ProjectionConfiguration<TProjection>(version);
        build(builder);
        if (!_projections.TryAdd(type, builder)) throw new AlreadyExistsException();
        return this;
    }
}