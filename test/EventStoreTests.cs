using System.Globalization;
using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.Tests.Fixtures;
using Event = ServcoX.EventSauce.Events.Event;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new Wrapper();
        var aggregateId = NewId();
        await wrapper.Sut.Write(aggregateId, TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.Write(aggregateId, TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.Write(aggregateId, TestPayloads.C, TestMetadata.C);

        using var reader = wrapper.GetSliceStream(0);
        AssertEvent(reader, aggregateId, TestPayloads.A, TestMetadata.A);
        AssertEvent(reader, aggregateId, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader, aggregateId, TestPayloads.C, TestMetadata.C);
    }


    [Fact]
    public async Task CanWriteMultiples()
    {
        using var wrapper = new Wrapper();
        var aggregateId = NewId();
        await wrapper.Sut.Write(new List<Event>
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
        using var wrapper = new Wrapper();
        wrapper.PrepareForOverlappingWrite();

        var aggregateId = NewId();
        await wrapper.Sut.Write(aggregateId, TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.Write(aggregateId, TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.Write(aggregateId, TestPayloads.C, TestMetadata.C);

        using var reader0 = wrapper.GetSliceStream(0);
        for (var i = 0; i < Wrapper.MaxBlocksPerSlice - 1; i++) reader0.ReadLine()!.Should().BeEmpty();
        AssertEvent(reader0, aggregateId, TestPayloads.A, TestMetadata.A);

        using var reader1 = wrapper.GetSliceStream(1);
        AssertEvent(reader1, aggregateId, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader1, aggregateId, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(1);
        AssertEvent(events[0], TestEvents.A, 0, 0, 108);
    }

    [Fact]
    public async Task CanReadMultiples()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(0, TestEvents.BEncoded);
        wrapper.AppendLine(0, TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(3);
        AssertEvent(events[0], TestEvents.A, 0, 0, 108);
        AssertEvent(events[1], TestEvents.B, 0, 108, 229);
        AssertEvent(events[2], TestEvents.C, 0, 229, 345);
    }

    [Fact]
    public async Task CanReadWithMax()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(0, TestEvents.BEncoded);
        wrapper.AppendLine(0, TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read(maxEvents: 2)).ToList();
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.A, 0, 0, 108);
        AssertEvent(events[1], TestEvents.B, 0, 108, 229);
    }

    [Fact]
    public async Task CanReadWithOffset()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(0, TestEvents.BEncoded);
        wrapper.AppendLine(0, TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read(startSliceOffset: 108)).ToList();
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.B, 0, 108, 229);
        AssertEvent(events[1], TestEvents.C, 0, 229, 345);
    }

    [Fact]
    public async Task CanReadOverlappingSlice()
    {
        using var wrapper = new Wrapper();
        wrapper.PrepareForOverlappingWrite();

        wrapper.AppendLine(0, TestEvents.AEncoded);
        wrapper.AppendLine(1, TestEvents.BEncoded);
        wrapper.AppendLine(1, TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(3);
        AssertEvent(events[0], TestEvents.A, 0, Wrapper.MaxBlocksPerSlice - 1, Wrapper.MaxBlocksPerSlice + 107);
        AssertEvent(events[1], TestEvents.B, 1, 0, 121);
        AssertEvent(events[2], TestEvents.C, 1, 121, 237);
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

    private static void AssertEvent(IEgressEvent actual, IEgressEvent expected, UInt64 slice, UInt64 offset, UInt64 nextOffset)
    {
        actual.AggregateId.Should().Be(expected.AggregateId);
        actual.Type.Should().BeEquivalentTo(expected.Type);
        actual.Payload.Should().Be(expected.Payload);
        actual.Metadata.Should().BeEquivalentTo(expected.Metadata);
        actual.At.Should().Be(expected.At);
        actual.Slice.Should().Be(slice);
        actual.Offset.Should().Be(offset);
        actual.NextOffset.Should().Be(nextOffset);
    }

    private static String NewId() => Guid.NewGuid().ToString("N");
}