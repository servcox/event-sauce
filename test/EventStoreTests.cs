using System.Globalization;
using System.Text.Json;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;
using ServcoX.EventSauce.Tests.Fixtures;
using Stream = System.IO.Stream;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    private static readonly Byte[] Buffer = "BAKEDCAKE\t20240108T012117Z\t{}\tnull\n"u8.ToArray();

    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.Write(TestPayloads.A, TestMetadata.A);
        await wrapper.Sut.Write(TestPayloads.B, TestMetadata.B);
        await wrapper.Sut.Write(TestPayloads.C, TestMetadata.C);

        await using var stream = wrapper.GetBlob(TestSlices.First);

        await AssertEvent(stream, TestPayloads.A, TestMetadata.A);
        await AssertEvent(stream, TestPayloads.B, TestMetadata.B);
        await AssertEvent(stream, TestPayloads.C, TestMetadata.C);
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

        await using var stream = wrapper.GetBlob(TestSlices.First);

        await AssertEvent(stream, TestPayloads.A, TestMetadata.A);
        await AssertEvent(stream, TestPayloads.B, TestMetadata.B);
        await AssertEvent(stream, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanWriteOverlapping()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        await blob.CreateAsync();

        for (var i = 0; i < 49_999; i++)
        {
            blob.AppendBlock(Buffer);
        }

        await wrapper.Sut.Write(new List<Event<Dictionary<String, String>>>()
        {
            new(TestPayloads.A, TestMetadata.A),
            new(TestPayloads.B, TestMetadata.B),
            new(TestPayloads.C, TestMetadata.C),
        });

        await using var stream = wrapper.GetBlob(TestSlices.First);
        var reader = new StreamReader(stream);
        for (var i = 0; i < 49_999; i++)
        {
            var line = (await reader.ReadLineAsync())!;
            line.Should().NotBeNull();
        }

        await AssertEvent(stream, TestPayloads.A, TestMetadata.A);
        await AssertEvent(stream, TestPayloads.B, TestMetadata.B);
        await AssertEvent(stream, TestPayloads.C, TestMetadata.C);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        wrapper.AppendBlock(TestSlices.First,TestEvents.AEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(1);
        events[0].Should().BeEquivalentTo(TestEvents.A);
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
        events[0].Should().BeEquivalentTo(TestEvents.A);
        events[1].Should().BeEquivalentTo(TestEvents.B);
        events[2].Should().BeEquivalentTo(TestEvents.C);
    }

    [Fact]
    public async Task CanReadLimited()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read(2)).ToList();
        events.Count.Should().Be(2);
        events[0].Should().BeEquivalentTo(TestEvents.A);
        events[1].Should().BeEquivalentTo(TestEvents.B);
    }

    [Fact]
    public async Task CanReadOverlappingSlice()
    {
        using var wrapper = new Wrapper();
        var blob = wrapper.Container.GetAppendBlobClient(TestSlices.First);
        await blob.CreateIfNotExistsAsync();
        for (var i = 0; i < 49_999; i++)
        {
            blob.AppendBlock(TestEvents.AEncoded);
        }

        blob.AppendBlock(TestEvents.AEncoded);
        blob.AppendBlock(TestEvents.BEncoded);
        blob.AppendBlock(TestEvents.CEncoded);

        var events = (await wrapper.Sut.Read()).ToList();
        events.Count.Should().Be(49_999 + 3);
        events[49_999].Should().BeEquivalentTo(TestEvents.A);
        events[49_999 + 1].Should().BeEquivalentTo(TestEvents.B);
        events[49_999 + 2].Should().BeEquivalentTo(TestEvents.C);
    }

    private static async Task AssertEvent(Stream stream, Object payload, Object metadata)
    {
        var reader = new StreamReader(stream);
        var line = (await reader.ReadLineAsync())!;
        line.Should().NotBeNull();

        var tokens = line.Split("\t");
        tokens[0].Should().Be(payload.GetType().FullName!.ToUpperInvariant());
        DateTime.ParseExact(tokens[1], @"yyyyMMdd\THHmmss\Z", CultureInfo.InvariantCulture).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
        tokens[2].Should().BeEquivalentTo(JsonSerializer.Serialize(payload));
        tokens[3].Should().BeEquivalentTo(JsonSerializer.Serialize(metadata));
    }
}