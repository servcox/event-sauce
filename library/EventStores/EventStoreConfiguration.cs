using System.Collections.ObjectModel;
using System.Text.Json;
using System.Text.Json.Serialization;

// ReSharper disable UnusedMethodReturnValue.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global

namespace ServcoX.EventSauce.EventStores;

public sealed class EventStoreConfiguration
{
    public String StreamTableName { get; set; } = "stream";
    public String EventTableName { get; set; } = "event";
    public String ProjectionTableName { get; set; } = "projection";
    public Boolean CreateTablesIfMissing { get; set; }
    public TimeSpan? CheckUnprojectedEventsInterval { get; set; }

    public Dictionary<String, Collection<IProjectionBuilder>> Projections { get; } = new();

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

    public EventStoreConfiguration DefineProjection<TProjection>(String streamType, Action<ProjectionBuilder<TProjection>> build)
    {
        ArgumentNullException.ThrowIfNull(build);
        if (!Projections.TryGetValue(streamType, out var set)) set = Projections[streamType] = new();
        if (set.Any(a => a.Type == typeof(TProjection))) throw new AlreadyExistsException();

        var builder = new ProjectionBuilder<TProjection>();
        build(builder);
        set.Add(builder);
        return this;
    }
}