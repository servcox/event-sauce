using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tests.TestData;

namespace ServcoX.EventSauce.Tests;

public class EventStoreEventTests
{
    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new Wrapper();
        var evt = new TestAEvent("a");
        await wrapper.Sut.WriteEvents(Wrapper.StreamId1, evt, Wrapper.UserId, CancellationToken.None);
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
        await Assert.ThrowsAsync<NotFoundException>(async () => await wrapper.Sut.WriteEvents(Guid.NewGuid().ToString("N"), new TestAEvent("a"), Wrapper.UserId, CancellationToken.None));
    }

    [Fact]
    public async Task CanFailOptimisticWrite()
    {
        using var wrapper = new Wrapper();
        // The following simulates a concurrency issue - the DB is broken after this!
        await wrapper.EventTable.AddEntityAsync<EventRecord>(new()
        {
            StreamId = Wrapper.StreamId1,
            Version = 4,
            CreatedBy = Wrapper.UserId,
        });
        await Assert.ThrowsAsync<OptimisticWriteInterruptedException>(async () => await wrapper.Sut.WriteEvents(Wrapper.StreamId1, new TestAEvent(), Wrapper.UserId, CancellationToken.None));
    }

    [Fact]
    public void CanRead()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadEvents(Wrapper.StreamId1).ToList();
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
    public void CanReadPartial()
    {
        using var wrapper = new Wrapper();
        var events = wrapper.Sut.ReadEvents(Wrapper.StreamId1, 3).ToList();
        events.Count.Should().Be(1);
        events[0].StreamId.Should().Be(Wrapper.StreamId1);
        events[0].Type.Should().Be(Wrapper.EventType3.ToUpperInvariant());
        events[0].Version.Should().Be(3);
        events[0].Body.Should().Be(Wrapper.EventBody3);
        events[0].CreatedBy.Should().Be(Wrapper.UserId);
        events[0].CreatedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
    }
}

