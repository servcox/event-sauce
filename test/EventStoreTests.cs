using System.Text.Json;
using Azure.Data.Tables;
using FluentAssertions;
using Microsoft.VisualBasic.CompilerServices;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.TableRecords;

// ReSharper disable NotAccessedPositionalProperty.Global

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    private const String DevelopmentConnectionString = "UseDevelopmentStorage=true;";

    private const String StreamType1 = "stream-type-a";
    private const String StreamType2 = "stream-type-b";
    private const String StreamId1 = "stream-1";
    private const String StreamId2 = "stream-2";
    private const String StreamId3 = "stream-3";

    private static readonly List<EventStreamRecord> Streams = new()
    {
        new(StreamId1, StreamType1.ToUpperInvariant(), 3, false),
        new(StreamId2, StreamType1.ToUpperInvariant(), 0, false),
        new(StreamId3, StreamType2.ToUpperInvariant(), 0, false),
    };

    private const String UserId = "user-1";
    private static readonly String EventType1 = typeof(TestAEvent).FullName!;
    private static readonly String EventType2 = typeof(TestBEvent).FullName!;
    private static readonly String EventType3 = typeof(TestCEvent).FullName!;
    private static readonly IEventBody EventBody1 = new TestAEvent("1");
    private static readonly IEventBody EventBody2 = new TestBEvent("2");
    private static readonly IEventBody EventBody3 = new TestCEvent("3");

    private static readonly List<EventRecord> Events = new()
    {
        new(StreamId1, 1, EventType1.ToUpperInvariant(), JsonSerializer.Serialize((Object)EventBody1), UserId),
        new(StreamId1, 2, EventType2.ToUpperInvariant(), JsonSerializer.Serialize((Object)EventBody2), UserId),
        new(StreamId1, 3, EventType3.ToUpperInvariant(), JsonSerializer.Serialize((Object)EventBody3), UserId),
    };

    [Fact]
    public void CanListStreams()
    {
        using var wrapper = new Wrapper();
        var streams = wrapper.Sut.ListStreams(StreamType1).ToArray();
        streams.Length.Should().Be(2);
        streams[0].Id.Should().Be(StreamId1);
        streams[0].Type.Should().Be(StreamType1.ToUpperInvariant());
        streams[1].Id.Should().Be(StreamId2);
        streams[1].Type.Should().Be(StreamType1.ToUpperInvariant());
    }

    [Fact]
    public async Task CanCreateStream()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, StreamType1, CancellationToken.None);
        var stream = wrapper.StreamTable.Query<EventStreamRecord>()
            .Single(stream => stream.PartitionKey == streamId && stream.RowKey == streamId);

        stream.Type.Should().Be(StreamType1.ToUpperInvariant());
        stream.LatestVersion.Should().Be(0);
        stream.IsArchived.Should().BeFalse();
    }

    [Fact]
    public async Task CanNotCreateStreamTwice()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, StreamType1, CancellationToken.None);
        await Assert.ThrowsAsync<AlreadyExistsException>(async () => await wrapper.Sut.CreateStream(streamId, StreamType1, CancellationToken.None));
    }

    [Fact]
    public async Task CanTryCreateStream()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStreamIfNotExist(streamId, StreamType1, CancellationToken.None);
        var stream = wrapper.StreamTable.Query<EventStreamRecord>()
            .Single(stream => stream.PartitionKey == streamId && stream.RowKey == streamId);

        stream.Type.Should().Be(StreamType1.ToUpperInvariant());
        stream.LatestVersion.Should().Be(0);
        stream.IsArchived.Should().BeFalse();
    }

    [Fact]
    public async Task DoesNotErrorWhenStreamAlreadyExists()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, StreamType1, CancellationToken.None);
        await wrapper.Sut.CreateStreamIfNotExist(streamId, StreamType1, CancellationToken.None);
    }

    [Fact]
    public async Task CanArchiveStream()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.ArchiveStream(StreamId1, CancellationToken.None);
        var stream = wrapper.StreamTable.GetEntity<EventStreamRecord>(StreamId1, StreamId1).Value;
        stream.LatestVersion.Should().Be(3);
        stream.IsArchived.Should().BeTrue();
    }

    [Fact]
    public async Task CanWriteStream()
    {
        using var wrapper = new Wrapper();
        var evt = new TestAEvent("a");
        await wrapper.Sut.WriteStream(StreamId1, evt, UserId, CancellationToken.None);
        var stream = wrapper.StreamTable.GetEntity<EventStreamRecord>(StreamId1, StreamId1).Value;
        stream.LatestVersion.Should().Be(4);

        var events = wrapper.EventTable.Query<EventRecord>(e => e.PartitionKey == StreamId1);
        events.Count().Should().Be(4);
        var evt2 = events.Last();
        evt2.PartitionKey.Should().Be(StreamId1);
        evt2.RowKey.Should().Be("00000000000000000004");
        evt2.Type.Should().Be(EventType1.ToUpperInvariant());
        evt2.Body.Should().Be(JsonSerializer.Serialize(evt));
        evt2.CreatedBy.Should().Be(UserId);
    }

    [Fact]
    public async Task CanNotWriteToMissingStream()
    {
        using var wrapper = new Wrapper();
        await Assert.ThrowsAsync<NotFoundException>(async () => await wrapper.Sut.WriteStream(Guid.NewGuid().ToString("N"), new TestAEvent("a"), UserId, CancellationToken.None));
    }

    [Fact]
    public async Task CanFailOptimisticStreamWrite()
    {
        using var wrapper = new Wrapper();
        // The following simulates a concurrency issue - the DB is broken after this!
        await wrapper.EventTable.AddEntityAsync<EventRecord>(new(StreamId1, 4, string.Empty, string.Empty, UserId));
        await Assert.ThrowsAsync<OptimisticWriteInterruptedException>(async () => await wrapper.Sut.WriteStream(StreamId1, new TestAEvent(), UserId, CancellationToken.None));
    }

    [Fact]
    public void CanReadStream()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadStream(StreamId1).ToList();
        events.Count.Should().Be(3);

        events[0].StreamId.Should().Be(StreamId1);
        events[0].Type.Should().Be(EventType1.ToUpperInvariant());
        events[0].Version.Should().Be(1);
        events[0].Body.Should().Be(EventBody1);
        events[0].CreatedBy.Should().Be(UserId);
        events[0].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));

        events[1].StreamId.Should().Be(StreamId1);
        events[1].Type.Should().Be(EventType2.ToUpperInvariant());
        events[1].Version.Should().Be(2);
        events[1].Body.Should().Be(EventBody2);
        events[1].CreatedBy.Should().Be(UserId);
        events[1].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));

        events[2].StreamId.Should().Be(StreamId1);
        events[2].Type.Should().Be(EventType3.ToUpperInvariant());
        events[2].Version.Should().Be(3);
        events[2].Body.Should().Be(EventBody3);
        events[2].CreatedBy.Should().Be(UserId);
        events[2].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void CanReadPartialStream()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadStream(StreamId1, 3).ToList();
        events.Count.Should().Be(1);
        events[0].StreamId.Should().Be(StreamId1);
        events[0].Type.Should().Be(EventType3.ToUpperInvariant());
        events[0].Version.Should().Be(3);
        events[0].Body.Should().Be(EventBody3);
        events[0].CreatedBy.Should().Be(UserId);
        events[0].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void CanReadProjection()
    {
        using var wrapper = new Wrapper();

        var prj = wrapper.Sut.ReadProjection<Projection>(StreamId1);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);
    }

    private class Wrapper : IDisposable
    {
        public TableClient StreamTable { get; }
        public TableClient EventTable { get; }
        public TableClient ProjectionTable { get; }
        public EventStore Sut { get; }

        public Wrapper()
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

            Sut = new(DevelopmentConnectionString, cfg => cfg
                .UseStreamTable(streamTableName)
                .UseEventTable(eventTableName)
                .UseProjectionTable(projectionTableName)
                .DefineProjection<Projection>(1, builder => builder
                    .On<TestAEvent>((prj, body, evt) => prj.A++)
                    .On<TestBEvent>((prj, body, evt) => prj.B++)
                    .OnOther((prj, evt) => prj.Other++)
                    .OnAny((prj, evt) => prj.Any++)
                )
            );
        }

        public void Dispose()
        {
            StreamTable.Delete();
            EventTable.Delete();
            ProjectionTable.Delete();
        }
    }
}

public readonly record struct TestAEvent(String A) : IEventBody;

public readonly record struct TestBEvent(String B) : IEventBody;

public readonly record struct TestCEvent(String C) : IEventBody;

public record Projection
{
    public Int32 A { get; set; }
    public Int32 B { get; set; }
    public Int32 Other { get; set; }
    public Int32 Any { get; set; }
}