using System.Globalization;
using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;
using ServcoX.EventSauce.Extensions;
using ServcoX.EventSauce.Tests.Fixtures;
using Stream = System.IO.Stream;

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
        await AssertEvent(reader, TestPayloads.A, TestMetadata.A);
        await AssertEvent(reader, TestPayloads.B, TestMetadata.B);
        await AssertEvent(reader, TestPayloads.C, TestMetadata.C);
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
        await AssertEvent(reader, TestPayloads.A, TestMetadata.A);
        await AssertEvent(reader, TestPayloads.B, TestMetadata.B);
        await AssertEvent(reader, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteOverlapping()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        await blob.CreateAsync();

        await PrepareForOverlappingWrite(blob);

        await wrapper.Sut.Write(new List<Event<Dictionary<String, String>>>()
        {
            new(TestPayloads.A, TestMetadata.A),
            new(TestPayloads.B, TestMetadata.B),
            new(TestPayloads.C, TestMetadata.C),
        });

        using var reader = wrapper.GetSliceStream(0);
        await AssertEvent(reader, TestPayloads.A, TestMetadata.A);
        await AssertEvent(reader, TestPayloads.B, TestMetadata.B);
        await AssertEvent(reader, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendBlock(0, TestEvents.AEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(1);
        AssertEvent(events[0], TestEvents.A, 0, 0, 106);
    }

    [Fact]
    public async Task CanReadMultiples()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

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

        var events = (await wrapper.Sut.Read(maxEvents:2)).ToList();
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
        AssertEvent(events[1], TestEvents.B, 0, 106, 225);
        AssertEvent(events[2], TestEvents.C, 0, 225, 339);
    }

    [Fact]
    public async Task CanReadOverlappingSlice()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        await blob.CreateIfNotExistsAsync();
        
        await PrepareForOverlappingWrite(blob);

        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(49_999 + 3);
        events[49_999].Should().BeEquivalentTo(TestEvents.A);
        events[49_999 + 1].Should().BeEquivalentTo(TestEvents.B);
        events[49_999 + 2].Should().BeEquivalentTo(TestEvents.C);
    }

    private static async Task PrepareForOverlappingWrite(AppendBlobClient blob)
    {
        using var stream = new MemoryStream("\n"u8.ToArray());
        for (var i = 0; i < 999; i++)
        {
            await blob.AppendBlockAsync(stream);
            stream.Rewind();
        }
    }

    private static async Task AssertEvent(StreamReader reader, Object payload, Object metadata)
    {
        var line = (await reader.ReadLineAsync())!;
        line.Should().NotBeNull();

        var tokens = line.Split("\t");
        tokens[0].Should().Be(payload.GetType().FullName!.ToUpperInvariant());
        DateTime.ParseExact(tokens[1], @"yyyyMMdd\THHmmss\Z", CultureInfo.InvariantCulture).Should()
            .BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        tokens[2].Should().BeEquivalentTo(JsonSerializer.Serialize(payload));
        tokens[3].Should().BeEquivalentTo(JsonSerializer.Serialize(metadata));
    }

    private static void AssertEvent(IEgressEvent<Dictionary<String, String>> actual,
        IEgressEvent<Dictionary<String, String>> expected, UInt64 slice, UInt64 offset, UInt64 nextOffset)
    {
        actual.Type.Should().BeEquivalentTo(expected.Type);
        actual.Payload.Should().BeEquivalentTo(expected.Payload);
        actual.Metadata.Should().BeEquivalentTo(expected.Metadata);
        actual.At.Should().Be(expected.At);
        actual.Slice.Should().Be(slice);
        actual.Offset.Should().Be(offset);
        actual.NextOffset.Should().Be(nextOffset);
    }
}