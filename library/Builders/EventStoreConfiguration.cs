using System.Text.Json;
using System.Text.Json.Serialization;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace ServcoX.EventSauce.Builders;

public sealed class EventStoreConfiguration
{
    public String StreamTableName { get; set; } = "stream";
    public String EventTableName { get; set; } = "event";
    public String ProjectionTableName { get; set; } = "projection";
    public Boolean ShouldCreateTableIfMissing { get; set; }
    public TimeSpan? CheckUnprojectedEventsInterval { get; set; }

    public Dictionary<Type, IProjectionBuilder> Projections { get; } = new();

    public JsonSerializerOptions SerializationOptions { get; set; } = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
    };

    public EventStoreConfiguration UseStreamTable(String name)
    {
        StreamTableName = name;
        return this;
    }

    public EventStoreConfiguration UseEventTable(String name)
    {
        EventTableName = name;
        return this;
    }

    public EventStoreConfiguration UseProjectionTable(String name)
    {
        ProjectionTableName = name;
        return this;
    }

    public EventStoreConfiguration CheckForUnprojectedEventsEvery(TimeSpan every)
    {
        CheckUnprojectedEventsInterval = every;
        return this;
    }

    public EventStoreConfiguration CreateTablesIfMissing()
    {
        ShouldCreateTableIfMissing = true;
        return this;
    }

    public EventStoreConfiguration DefineProjection<TProjection>(String streamType, UInt64 version, Action<ProjectionBuilder<TProjection>> build) where TProjection : new()
    {
        ArgumentNullException.ThrowIfNull(build);
        // TODO: Where to store streamType?
        // TODO: Where to store version?
        
        var type = typeof(TProjection);
        var builder = new ProjectionBuilder<TProjection>();
        build(builder);
        if (!Projections.TryAdd(type, builder)) throw new AlreadyExistsException();
        return this;
    }
}