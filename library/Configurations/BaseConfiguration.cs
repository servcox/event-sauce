using System.Text.Json;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace ServcoX.EventSauce.Configurations;

public sealed class BaseConfiguration
{
    public JsonSerializerOptions SerializationOptions { get; set; } = new()
    {
        IgnoreNullValues = true,
        // If upgrading SDK: DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public String StreamTableName { get; set; } = "stream";

    public BaseConfiguration UseStreamTable(String name)
    {
        StreamTableName = name;
        return this;
    }

    public String EventTableName { get; set; } = "event";

    public BaseConfiguration UseEventTable(String name)
    {
        EventTableName = name;
        return this;
    }

    public String ProjectionTableName { get; set; } = "projection";

    public BaseConfiguration UseProjectionTable(String name)
    {
        ProjectionTableName = name;
        return this;
    }

    public Boolean ShouldCreateTableIfMissing { get; set; }

    public BaseConfiguration CreateTablesIfMissing()
    {
        ShouldCreateTableIfMissing = true;
        return this;
    }

    public TimeSpan? ProjectionRefreshInterval { get; set; }

    public BaseConfiguration RefreshProjectionsEvery(TimeSpan interval)
    {
        ProjectionRefreshInterval = interval;
        return this;
    }
    
    public Boolean ShouldRefreshProjectionsAfterWriting { get; set; }
    public BaseConfiguration RefreshProjectionsAfterWriting()
    {
        ShouldRefreshProjectionsAfterWriting = true;
        return this;
    }
    
    public Boolean ShouldRefreshProjectionsBeforeReading { get; set; }
    public BaseConfiguration RefreshProjectionBeforeReading()
    {
        ShouldRefreshProjectionsBeforeReading = true;
        return this;
    }

    public Dictionary<Type, IProjectionBuilder> Projections { get; } = new();

    public BaseConfiguration DefineProjection<TProjection>(String streamType, UInt32 version, Action<ProjectionConfiguration<TProjection>> build) where TProjection : new()
    {
        if (build is null) throw new ArgumentNullException(nameof(build));

        var type = typeof(TProjection);
        var builder = new ProjectionConfiguration<TProjection>(streamType, version);
        build(builder);
        if (Projections.ContainsKey(type)) throw new AlreadyExistsException();
        Projections.Add(type, builder);
        return this;
    }
}