using System.Globalization;
using System.Text.Json;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;
using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.Write(TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.Write(TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.Write(TestPayloads.C, TestMetadata.C);

        using var reader = wrapper.GetSliceStream(0);
        AssertEvent(reader, TestPayloads.A, TestMetadata.A);
        AssertEvent(reader, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteMultiples()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.Write(new List<Event<Dictionary<String, String>>>
        {
            new(TestPayloads.A, TestMetadata.A),
            new(TestPayloads.B, TestMetadata.B),
            new(TestPayloads.C, TestMetadata.C),
        });

        using var reader = wrapper.GetSliceStream(0);
        AssertEvent(reader, TestPayloads.A, TestMetadata.A);
        AssertEvent(reader, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteOverlapping()
    {
        using var wrapper = new Wrapper();
        wrapper.PrepareForOverlappingWrite();

        await wrapper.Sut.Write(TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.Write(TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.Write(TestPayloads.C, TestMetadata.C);

        using var reader0 = wrapper.GetSliceStream(0);
        for (var i = 0; i < 999; i++) reader0.ReadLine()!.Should().BeEmpty();
        AssertEvent(reader0, TestPayloads.A, TestMetadata.A);

        using var reader1 = wrapper.GetSliceStream(1);
        AssertEvent(reader1, TestPayloads.B, TestMetadata.B);
        AssertEvent(reader1, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendLine(0, TestEvents.AEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(1);
        AssertEvent(events[0], TestEvents.A, 0, 0, 106);
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
        AssertEvent(events[0], TestEvents.A, 0, 0, 106);
        AssertEvent(events[1], TestEvents.B, 0, 106, 225);
        AssertEvent(events[2], TestEvents.C, 0, 225, 339);
    }

    [Fact]
    public async Task CanReadWithMax()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read(maxEvents: 2)).ToList();
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.A, 0, 0, 71);
        AssertEvent(events[1], TestEvents.B, 0, 106, 225);
    }

    [Fact]
    public async Task CanReadWithOffset()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read(startSliceOffset: 106)).ToList();
        events.Count.Should().Be(2);
        AssertEvent(events[0], TestEvents.B, 0, 106, 225);
        AssertEvent(events[1], TestEvents.C, 0, 225, 339);
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
        AssertEvent(events[0], TestEvents.A, 0, 106, 225); // TODO: Offsets wrong
        AssertEvent(events[1], TestEvents.B, 1, 225, 339);
        AssertEvent(events[2], TestEvents.C, 1, 225, 339);
    }


    private static void AssertEvent(StreamReader reader, Object payload, Dictionary<String, String> metadata)
    {
        var line = reader.ReadLine()!;
        line.Should().NotBeNull();

        var tokens = line.Split("\t");
        tokens[0].Should().Be(payload.GetType().FullName!.ToUpperInvariant());
        DateTime.ParseExact(tokens[1], @"yyyyMMdd\THHmmss\Z", CultureInfo.InvariantCulture).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        tokens[2].Should().BeEquivalentTo(JsonSerializer.Serialize(payload));
        tokens[3].Should().BeEquivalentTo(JsonSerializer.Serialize(metadata));
    }

    private static void AssertEvent(IEgressEvent<Dictionary<String, String>> actual, IEgressEvent<Dictionary<String, String>> expected, UInt64 slice, UInt64 offset, UInt64 nextOffset)
    {
        actual.Type.Should().BeEquivalentTo(expected.Type);
        actual.Payload.Should().Be(expected.Payload);
        actual.Metadata.Should().BeEquivalentTo(expected.Metadata);
        actual.At.Should().Be(expected.At);
        actual.Slice.Should().Be(slice);
        actual.Offset.Should().Be(offset);
        actual.NextOffset.Should().Be(nextOffset);
    }
}