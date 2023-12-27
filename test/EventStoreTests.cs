using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tests.TestData;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public void CanListStreams()
    {
        using var wrapper = new Wrapper();
        var streams = wrapper.Sut.ListStreams(Wrapper.StreamType1).ToArray();
        streams.Length.Should().Be(2);
        streams[0].Id.Should().Be(Wrapper.StreamId1);
        streams[0].Type.Should().Be(Wrapper.StreamType1.ToUpperInvariant());
        streams[1].Id.Should().Be(Wrapper.StreamId2);
        streams[1].Type.Should().Be(Wrapper.StreamType1.ToUpperInvariant());
    }

    [Fact]
    public async Task CanCreateStream()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None);
        var stream = wrapper.StreamTable.Query<StreamRecord>()
            .Single(stream => stream.PartitionKey == streamId && stream.RowKey == streamId);

        stream.Type.Should().Be(Wrapper.StreamType1.ToUpperInvariant());
        stream.LatestVersion.Should().Be(0);
        stream.IsArchived.Should().BeFalse();
    }

    [Fact]
    public async Task CanNotCreateStreamTwice()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None);
        await Assert.ThrowsAsync<AlreadyExistsException>(async () => await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None));
    }

    [Fact]
    public async Task CanTryCreateStream()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStreamIfNotExist(streamId, Wrapper.StreamType1, CancellationToken.None);
        var stream = wrapper.StreamTable.Query<StreamRecord>()
            .Single(stream => stream.PartitionKey == streamId && stream.RowKey == streamId);

        stream.Type.Should().Be(Wrapper.StreamType1.ToUpperInvariant());
        stream.LatestVersion.Should().Be(0);
        stream.IsArchived.Should().BeFalse();
    }

    [Fact]
    public async Task DoesNotErrorWhenStreamAlreadyExists()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None);
        await wrapper.Sut.CreateStreamIfNotExist(streamId, Wrapper.StreamType1, CancellationToken.None);
    }

    [Fact]
    public async Task CanArchiveStream()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.ArchiveStream(Wrapper.StreamId1, CancellationToken.None);
        var stream = wrapper.StreamTable.GetEntity<StreamRecord>(Wrapper.StreamId1, Wrapper.StreamId1).Value;
        stream.LatestVersion.Should().Be(3);
        stream.IsArchived.Should().BeTrue();
    }

    [Fact]
    public async Task CanWriteStream()
    {
        using var wrapper = new Wrapper();
        var evt = new TestAEvent("a");
        await wrapper.Sut.WriteStream(Wrapper.StreamId1, evt, Wrapper.UserId, CancellationToken.None);
        var stream = wrapper.StreamTable.GetEntity<StreamRecord>(Wrapper.StreamId1, Wrapper.StreamId1).Value;
        stream.LatestVersion.Should().Be(4);

        var events = wrapper.EventTable.Query<EventRecord>(e => e.PartitionKey == Wrapper.StreamId1);
        events.Count().Should().Be(4);
        var evt2 = events.Last();
        evt2.PartitionKey.Should().Be(Wrapper.StreamId1);
        evt2.RowKey.Should().Be("00000000000000000004");
        evt2.Type.Should().Be(Wrapper.EventType1.ToUpperInvariant());
        evt2.Body.Should().Be(JsonSerializer.Serialize(evt));
        evt2.CreatedBy.Should().Be(Wrapper.UserId);
    }

    [Fact]
    public async Task CanNotWriteToMissingStream()
    {
        using var wrapper = new Wrapper();
        await Assert.ThrowsAsync<NotFoundException>(async () => await wrapper.Sut.WriteStream(Guid.NewGuid().ToString("N"), new TestAEvent("a"), Wrapper.UserId, CancellationToken.None));
    }

    [Fact]
    public async Task CanFailOptimisticStreamWrite()
    {
        using var wrapper = new Wrapper();
        // The following simulates a concurrency issue - the DB is broken after this!
        await wrapper.EventTable.AddEntityAsync<EventRecord>(new()
        {
            StreamId = Wrapper.StreamId1,
            Version = 4,
            CreatedBy = Wrapper.UserId,
        });
        await Assert.ThrowsAsync<OptimisticWriteInterruptedException>(async () => await wrapper.Sut.WriteStream(Wrapper.StreamId1, new TestAEvent(), Wrapper.UserId, CancellationToken.None));
    }

    [Fact]
    public void CanReadStream()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadStream(Wrapper.StreamId1).ToList();
        events.Count.Should().Be(3);

        events[0].StreamId.Should().Be(Wrapper.StreamId1);
        events[0].Type.Should().Be(Wrapper.EventType1.ToUpperInvariant());
        events[0].Version.Should().Be(1);
        events[0].Body.Should().Be(Wrapper.EventBody1);
        events[0].CreatedBy.Should().Be(Wrapper.UserId);
        events[0].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));

        events[1].StreamId.Should().Be(Wrapper.StreamId1);
        events[1].Type.Should().Be(Wrapper.EventType2.ToUpperInvariant());
        events[1].Version.Should().Be(2);
        events[1].Body.Should().Be(Wrapper.EventBody2);
        events[1].CreatedBy.Should().Be(Wrapper.UserId);
        events[1].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));

        events[2].StreamId.Should().Be(Wrapper.StreamId1);
        events[2].Type.Should().Be(Wrapper.EventType3.ToUpperInvariant());
        events[2].Version.Should().Be(3);
        events[2].Body.Should().Be(Wrapper.EventBody3);
        events[2].CreatedBy.Should().Be(Wrapper.UserId);
        events[2].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void CanReadPartialStream()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadStream(Wrapper.StreamId1, 3).ToList();
        events.Count.Should().Be(1);
        events[0].StreamId.Should().Be(Wrapper.StreamId1);
        events[0].Type.Should().Be(Wrapper.EventType3.ToUpperInvariant());
        events[0].Version.Should().Be(3);
        events[0].Body.Should().Be(Wrapper.EventBody3);
        events[0].CreatedBy.Should().Be(Wrapper.UserId);
        events[0].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
    }
}

