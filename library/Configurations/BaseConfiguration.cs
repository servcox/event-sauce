using System.Text.Json;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace ServcoX.EventSauce.Configurations;

public sealed class BaseConfiguration
{
    public String StreamTableName { get; set; } = "stream";
    public String EventTableName { get; set; } = "event";
    public String ProjectionTableName { get; set; } = "projection";
    public Boolean ShouldCreateTableIfMissing { get; set; }

    public Dictionary<Type, IProjectionBuilder> Projections { get; } = new();

    public JsonSerializerOptions SerializationOptions { get; set; } = new()
    {
        IgnoreNullValues = true,
        // If upgrading SDK: DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public BaseConfiguration UseStreamTable(String name)
    {
        StreamTableName = name;
        return this;
    }

    public BaseConfiguration UseEventTable(String name)
    {
        EventTableName = name;
        return this;
    }

    public BaseConfiguration UseProjectionTable(String name)
    {
        ProjectionTableName = name;
        return this;
    }

    public BaseConfiguration CreateTablesIfMissing()
    {
        ShouldCreateTableIfMissing = true;
        return this;
    }

    public BaseConfiguration DefineProjection<TProjection>(UInt32 version, Action<ProjectionConfiguration<TProjection>> build) where TProjection : new()
    {
        if (build is null) throw new ArgumentNullException(nameof(build));
        
        var type = typeof(TProjection);
        var builder = new ProjectionConfiguration<TProjection>(version);
        build(builder);
        if (Projections.ContainsKey(type)) throw new AlreadyExistsException();
        Projections.Add(type, builder);
        return this;
    }
}