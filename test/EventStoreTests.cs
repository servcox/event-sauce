using System.Globalization;
using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.Models;
using ServcoX.EventSauce.Tests.Fixtures;
using Event = ServcoX.EventSauce.Models.Event;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new EventWrapper();
        var aggregateId = NewId();
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.C, TestMetadata.C);

        using var reader = wrapper.GetSliceStream(0);
        AssertEvent(reader, aggregateId, TestPayloads.A, TestMetadata.A);
        AssertEvent(reader, aggregateId, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader, aggregateId, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteMultiples()
    {
        using var wrapper = new EventWrapper();
        var aggregateId = NewId();
        await wrapper.Sut.WriteEvents(new List<Event>
        {
            new(aggregateId, TestPayloads.A, TestMetadata.A),
            new(aggregateId, TestPayloads.B, TestMetadata.B),
            new(aggregateId, TestPayloads.C, TestMetadata.C),
        });

        using var reader = wrapper.GetSliceStream(0);
        AssertEvent(reader, aggregateId, TestPayloads.A, TestMetadata.A);
        AssertEvent(reader, aggregateId, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader, aggregateId, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteOverlapping()
    {
        using var wrapper = new EventWrapper();
        wrapper.PrepareForOverlappingWrite();

        var aggregateId = NewId();
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.WriteEvent(aggregateId, TestPayloads.C, TestMetadata.C);

        using var reader0 = wrapper.GetSliceStream(0);
        for (var i = 0; i < EventWrapper.MaxBlocksPerSlice - 1; i++) reader0.ReadLine()!.Should().BeEmpty();
        AssertEvent(reader0, aggregateId, TestPayloads.A, TestMetadata.A);

        using var reader1 = wrapper.GetSliceStream(1);
        AssertEvent(reader1, aggregateId, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader1, aggregateId, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new EventWrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(1);
        AssertEvent(events[0], TestEvents.A, 0, 0, 108);
    }

    [Fact]
    public async Task CanReadMultiples()
    {
        using var wrapper = new EventWrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(0, TestEvents.BEncoded);
        wrapper.AppendLine(0, TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(3);
        AssertEvent(events[0], TestEvents.A, 0, 0, 108);
        AssertEvent(events[1], TestEvents.B, 0, 108, 229);
        AssertEvent(events[2], TestEvents.C, 0, 229, 345);
    }

    [Fact]
    public async Task CanReadWithOffset()
    {
        using var wrapper = new EventWrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(0, TestEvents.BEncoded);
        wrapper.AppendLine(0, TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0, 108);
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.B, 0, 108, 229);
        AssertEvent(events[1], TestEvents.C, 0, 229, 345);
    }

    [Fact]
    public async Task CanReadOverlappingSlice()
    {
        using var wrapper = new EventWrapper();
        wrapper.PrepareForOverlappingWrite();

        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(1, TestEvents.BEncoded);
        wrapper.AppendLine(1, TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(1);
        AssertEvent(events[0], TestEvents.A, 0, EventWrapper.MaxBlocksPerSlice - 1, EventWrapper.MaxBlocksPerSlice + 107);
        
        events = await wrapper.Sut.ReadEvents(1);
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.B, 1, 0, 121);
        AssertEvent(events[1], TestEvents.C, 1, 121, 237);
    }
    
    [Fact]
    public async Task CanListSLices()
    {
        using var wrapper = new EventWrapper();
        wrapper.PrepareForOverlappingWrite();

        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(1, TestEvents.BEncoded);
        wrapper.AppendLine(1, TestEvents.CEncoded);

        var slices = await wrapper.Sut.ListSlices();
        slices.Count.Should().Be(2);
        slices[0].Id.Should().Be(0);
        slices[1].Id.Should().Be(1);
    }

    private static void AssertEvent(StreamReader reader, String aggregateId, Object payload, Dictionary<String, String> metadata)
    {
        var line = reader.ReadLine()!;
        line.Should().NotBeNull();

        var tokens = line.Split("\t");
        tokens[0].Should().Be(aggregateId);
        tokens[1].Should().Be(payload.GetType().FullName!.ToUpperInvariant());
        DateTime.ParseExact(tokens[2], @"yyyyMMdd\THHmmss\Z", CultureInfo.InvariantCulture).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        tokens[3].Should().BeEquivalentTo(JsonSerializer.Serialize(payload));
        tokens[4].Should().BeEquivalentTo(JsonSerializer.Serialize(metadata));
    }

    private static void AssertEvent(IEgressEvent actual, IEgressEvent expected, Int64 sliceId, Int64 offset, Int64 nextOffset)
    {
        actual.AggregateId.Should().Be(expected.AggregateId);
        actual.Type.Should().BeEquivalentTo(expected.Type);
        actual.Payload.Should().Be(expected.Payload);
        actual.Metadata.Should().BeEquivalentTo(expected.Metadata);
        actual.At.Should().Be(expected.At);
        actual.SliceId.Should().Be(sliceId);
        actual.Offset.Should().Be(offset);
        actual.NextOffset.Should().Be(nextOffset);
    }

    private static String NewId() => Guid.NewGuid().ToString("N");
}