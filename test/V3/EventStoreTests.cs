using System.Globalization;
using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.Tests.V3.Fixtures;
using ServcoX.EventSauce.Tests.V3.TestData;

namespace ServcoX.EventSauce.Tests.V3;

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
    public async Task CanWriteZero()
    {
        using var wrapper = new EventWrapper();
        await wrapper.Sut.WriteEvents(new List<Event>());
    }
    
    [Fact]
    public async Task CanWriteWithSpecificAt()
    {
        using var wrapper = new EventWrapper();
        var aggregateId = NewId();
        var at = new DateTime(2001, 1, 1, 1, 1, 1, DateTimeKind.Utc);
        await wrapper.Sut.WriteEvent(aggregateId, new CakeBaked(), at:at );

        var events = await wrapper.Sut.ReadEvents(0);
        events[0].At.Should().Be(at);
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
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.AEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(1);
        AssertEvent(events[0], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.A);
    }

    [Fact]
    public async Task CanReadMultiples()
    {
        using var wrapper = new EventWrapper();
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.AEncoded);
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.BEncoded);
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(3);
        AssertEvent(events[0], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.A);
        AssertEvent(events[1], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.B);
        AssertEvent(events[2], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.C);
    }

    [Fact]
    public async Task CanReadWithOffset()
    {
        using var wrapper = new EventWrapper();
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.AEncoded);
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.BEncoded);
        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0, 111);
        events.Count.Should().Be(2);
        AssertEvent(events[0], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.B);
        AssertEvent(events[1], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.C);
    }

    [Fact]
    public async Task CanReadOverlappingSlice()
    {
        using var wrapper = new EventWrapper();
        wrapper.PrepareForOverlappingWrite();

        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.AEncoded);
        wrapper.AppendLine(1, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.BEncoded);
        wrapper.AppendLine(1, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.CEncoded);

        var events = await wrapper.Sut.ReadEvents(0);
        events.Count.Should().Be(1);
        AssertEvent(events[0], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.A);

        events = await wrapper.Sut.ReadEvents(1);
        events.Count.Should().Be(2);
        AssertEvent(events[0], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.B);
        AssertEvent(events[1], ServcoX.EventSauce.Tests.V3.TestData.TestEvents.C);
    }

    [Fact]
    public async Task CanListSlices()
    {
        using var wrapper = new EventWrapper();
        var slices = await wrapper.Sut.ListSlices();
        slices.Count.Should().Be(0);
        
        wrapper.PrepareForOverlappingWrite();
        
        slices = await wrapper.Sut.ListSlices();
        slices.Count.Should().Be(1);
        slices[0].Id.Should().Be(0);
        slices[0].End.Should().Be(9);

        wrapper.AppendLine(0, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.AEncoded);
        wrapper.AppendLine(1, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.BEncoded);
        wrapper.AppendLine(1, ServcoX.EventSauce.Tests.V3.TestData.TestEvents.CEncoded);

        slices = await wrapper.Sut.ListSlices();
        slices.Count.Should().Be(2);
        slices[0].Id.Should().Be(0);
        slices[0].End.Should().Be(120);
        slices[1].Id.Should().Be(1);
        slices[1].End.Should().Be(243);
    }

    private static void AssertEvent(TextReader reader, String aggregateId, Object payload, Dictionary<String, String> metadata)
    {
        var line = reader.ReadLine()!;
        line.Should().NotBeNull();

        var tokens = line.Split("\t");
        tokens[0].Should().Be(aggregateId);
        DateTime.ParseExact(tokens[1], @"yyyyMMdd\THHmmss\Z", CultureInfo.InvariantCulture).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
        tokens[2].Should().Be(payload.GetType().FullName!.ToUpperInvariant());
        tokens[3].Should().BeEquivalentTo(JsonSerializer.Serialize(payload));
        tokens[4].Should().BeEquivalentTo(JsonSerializer.Serialize(metadata));
    }

    private static void AssertEvent(IEgressEvent actual, IEgressEvent expected)
    {
        actual.AggregateId.Should().Be(expected.AggregateId);
        actual.Type.Should().BeEquivalentTo(expected.Type);
        actual.Payload.Should().Be(expected.Payload);
        actual.Metadata.Should().BeEquivalentTo(expected.Metadata);
        actual.At.Should().Be(expected.At);
    }

    private static String NewId() => Guid.NewGuid().ToString("N");
}