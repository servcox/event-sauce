using FluentAssertions;
using ServcoX.EventSauce.Exceptions;
using ServcoX.EventSauce.Tests.V2.TestData;
using ServcoX.EventSauce.V2.TableRecords;

namespace ServcoX.EventSauce.Tests.V2;

public class EventStoreStreamTests
{
    [Fact]
    public void CanList()
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
    public async Task CanCreate()
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
    public async Task CanNotCreateTwice()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None);
        await Assert.ThrowsAsync<AlreadyExistsException>(async () => await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None));
    }

    [Fact]
    public async Task CanTryCreate()
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
    public async Task DoesNotErrorWhenAlreadyExists()
    {
        using var wrapper = new Wrapper();
        var streamId = Guid.NewGuid().ToString("N");
        await wrapper.Sut.CreateStream(streamId, Wrapper.StreamType1, CancellationToken.None);
        await wrapper.Sut.CreateStreamIfNotExist(streamId, Wrapper.StreamType1, CancellationToken.None);
    }

    [Fact]
    public async Task CanArchive()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.ArchiveStream(Wrapper.StreamId1, CancellationToken.None);
        var stream = wrapper.StreamTable.GetEntity<StreamRecord>(Wrapper.StreamId1, Wrapper.StreamId1).Value;
        stream.LatestVersion.Should().Be(3);
        stream.IsArchived.Should().BeTrue();
    }

}

