namespace ServcoX.EventSauce.ConfigurationBuilders;

public class BaseProjectionConfiguration
{
    public TimeSpan CacheUpdateInterval { get; private set; }
    public BaseProjectionConfiguration UpdateCacheEvery(TimeSpan interval)
    {
        CacheUpdateInterval = interval;
        return this;
    }
    
    public Dictionary<Type, IProjectionBuilder> Projections { get; } = new();
    public BaseProjectionConfiguration DefineProjection<TProjection>(UInt32 version, Action<SpecificProjectionConfiguration<TProjection>> build) where TProjection : new()
    {
        if (build is null) throw new ArgumentNullException(nameof(build));

        var type = typeof(TProjection);
        var builder = new SpecificProjectionConfiguration<TProjection>(version);
        build(builder);
        if (Projections.ContainsKey(type)) throw new AlreadyExistsException();
        Projections.Add(type, builder);
        return this;
    }
}