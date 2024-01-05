using Azure;
using Azure.Data.Tables;
using FluentAssertions;
using ServcoX.EventSauce.Tests.TestData;
using ServcoX.EventSauce.V2.TableRecords;

// ReSharper disable ObjectCreationAsStatement

namespace ServcoX.EventSauce.Tests;

public class EventStoreProjectionTests
{
    [Fact]
    public void CanNotUseReservedIndexName() => Assert.Throws<InvalidIndexNameException>(() =>
    {
        new EventStore(Wrapper.DevelopmentConnectionString, cfg => cfg
            .DefineProjection<TestProjection>(Wrapper.StreamType1, Wrapper.ProjectionVersion, builder => builder
                .Index(nameof(ProjectionRecord.ProjectionId), prj => prj.A.ToString())
            ));
    });

    [Fact]
    public void CanNotUseTooLongIndexName() => Assert.Throws<InvalidIndexNameException>(() =>
    {
        new EventStore(Wrapper.DevelopmentConnectionString, cfg => cfg
            .DefineProjection<TestProjection>(Wrapper.StreamType1, Wrapper.ProjectionVersion, builder => builder
                .Index(new('a', 256), prj => prj.A.ToString())
            ));
    });


    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        var prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1);
        prj.Id.Should().Be(Wrapper.StreamId1);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);
    }

    [Fact]
    public async Task CanNotReadArchivedProjection()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.ArchiveStream(Wrapper.StreamId1);
        await Assert.ThrowsAsync<NotFoundException>(async () => await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1));
    }

    [Fact]
    public async Task CanNotReadMissing()
    {
        using var wrapper = new Wrapper();
        await Assert.ThrowsAsync<NotFoundException>(async () => await wrapper.Sut.ReadProjection<TestProjection>("bad id"));
    }

    [Fact]
    public async Task CanTryRead()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        var projection = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1);
        projection.Id.Should().Be(Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanTryReadMissing()
    {
        using var wrapper = new Wrapper();
        var projection = await wrapper.Sut.TryReadProjection<TestProjection>("bad id");
        projection.Should().BeNull();
    }

    [Fact]
    public async Task CanList()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>().ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanListWithFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "1").ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanFindNothingWithBadFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "2").ToList();
        projections.Count().Should().Be(0);
    }
}