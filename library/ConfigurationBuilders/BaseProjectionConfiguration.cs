namespace ServcoX.EventSauce.ConfigurationBuilders;

public sealed class BaseProjectionConfiguration
{
    public TimeSpan CacheUpdateInterval { get; private set; }

    public BaseProjectionConfiguration UpdateCacheEvery(TimeSpan interval)
    {
        CacheUpdateInterval = interval;
        return this;
    }

    private readonly Dictionary<Type, IProjectionBuilder> _projections = new();
    public IReadOnlyDictionary<Type, IProjectionBuilder> Projections => _projections;

    public BaseProjectionConfiguration DefineProjection<TProjection>(UInt32 version, Action<SpecificProjectionConfiguration<TProjection>> build) where TProjection : new()
    {
        if (build is null) throw new ArgumentNullException(nameof(build));

        var type = typeof(TProjection);
        var builder = new SpecificProjectionConfiguration<TProjection>(version);
        build(builder);
        if (!_projections.TryAdd(type, builder)) throw new AlreadyExistsException();
        return this;
    }
}