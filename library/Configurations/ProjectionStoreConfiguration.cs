namespace ServcoX.EventSauce.Configurations;

public sealed class ProjectionStoreConfiguration
{
    public TimeSpan CacheUpdateInterval { get; private set; }

    public ProjectionStoreConfiguration UpdateCacheEvery(TimeSpan interval)
    {
        CacheUpdateInterval = interval;
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