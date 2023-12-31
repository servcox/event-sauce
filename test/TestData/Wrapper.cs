using System.Text.Json;
using Azure.Data.Tables;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tests.TestData;

public sealed class Wrapper : IDisposable
{
    public const String StreamType1 = "stream-type-a";
    public const String StreamType2 = "stream-type-b";
    public const String StreamId1 = "stream-1";
    public const String StreamId2 = "stream-2";
    public const String StreamId3 = "stream-3";

    public static readonly List<StreamRecord> Streams = new()
    {
        new()
        {
            StreamId = StreamId1,
            Type = StreamType1.ToUpperInvariant(),
            LatestVersion = 3,
            IsArchived = false,
        },
        new()
        {
            StreamId = StreamId2,
            Type = StreamType1.ToUpperInvariant(),
            LatestVersion = 0,
            IsArchived = false,
        },
        new()
        {
            StreamId = StreamId3,
            Type = StreamType2.ToUpperInvariant(),
            LatestVersion = 0,
            IsArchived = false,
        },
    };

    public const String UserId = "user-1";
    public static readonly String EventType1 = typeof(TestAEvent).FullName!;
    public static readonly String EventType2 = typeof(TestBEvent).FullName!;
    public static readonly String EventType3 = typeof(TestCEvent).FullName!;
    public static readonly IEventBody EventBody1 = new TestAEvent("1");
    public static readonly IEventBody EventBody2 = new TestBEvent("2");
    public static readonly IEventBody EventBody3 = new TestCEvent("3");

    public static readonly List<EventRecord> Events = new()
    {
        new()
        {
            StreamId = StreamId1,
            Version = 1,
            Type = EventType1.ToUpperInvariant(),
            Body = JsonSerializer.Serialize((Object)EventBody1),
            CreatedBy = UserId,
        },
        new()
        {
            StreamId = StreamId1,
            Version = 2,
            Type = EventType2.ToUpperInvariant(),
            Body = JsonSerializer.Serialize((Object)EventBody2),
            CreatedBy = UserId,
        },
        new()
        {
            StreamId = StreamId1,
            Version = 3,
            Type = EventType3.ToUpperInvariant(),
            Body = JsonSerializer.Serialize((Object)EventBody3),
            CreatedBy = UserId,
        },
    };

    public const UInt32 ProjectionVersion = 1;

    public const String DevelopmentConnectionString = "UseDevelopmentStorage=true;";

    public TableClient StreamTable { get; }
    public TableClient EventTable { get; }
    public TableClient ProjectionTable { get; }
    public IEventStore Sut { get; }

    public Wrapper(Action<BaseConfiguration>? overwriteBuilder = null)
    {
        var postfix = Guid.NewGuid().ToString("N").ToUpperInvariant();
        var streamTableName = $"stream{postfix}";
        var eventTableName = $"event{postfix}";
        var projectionTableName = $"projection{postfix}";

        StreamTable = new(DevelopmentConnectionString, streamTableName);
        StreamTable.Create();
        foreach (var stream in Streams) StreamTable.AddEntity(stream);

        EventTable = new(DevelopmentConnectionString, eventTableName);
        EventTable.Create();
        foreach (var evt in Events) EventTable.AddEntity(evt);

        ProjectionTable = new(DevelopmentConnectionString, projectionTableName);
        ProjectionTable.Create();

        Sut = new EventStore(DevelopmentConnectionString, cfg =>
        {
            cfg
                .UseStreamTable(streamTableName)
                .UseEventTable(eventTableName)
                .UseProjectionTable(projectionTableName)
                .DefineProjection<TestProjection>(StreamType1, ProjectionVersion, builder => builder
                    .OnCreation((prj, id) => prj.Id = id)
                    .OnEvent<TestAEvent>((prj, _, _) => prj.A++)
                    .OnEvent<TestBEvent>((prj, _, _) => prj.B++)
                    .OnUnexpectedEvent((prj, _) => prj.Other++)
                    .OnAnyEvent((prj, _) => prj.Any++)
                    .Index("A", prj => prj.A.ToString())
                );
            overwriteBuilder?.Invoke(cfg);
        });
    }

    public void Dispose()
    {
        StreamTable.Delete();
        EventTable.Delete();
        ProjectionTable.Delete();
    }
}