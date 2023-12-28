using Azure.Data.Tables;
using FluentAssertions;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tests.TestData;
using ServcoX.EventSauce.Utilities;

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
    public async Task CanRefreshProjection()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);

        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);
        var record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId1).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(3);
        record.GetString("A").Should().Be("1"); // Indexed value
    }

    [Fact]
    public async Task CanAutoRefreshOnWrite()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionsAfterWriting());
        await wrapper.Sut.WriteEvents(Wrapper.StreamId2, new TestAEvent("a"), Wrapper.UserId);
        
        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);
        var record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId2).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(1);
        record.GetString("A").Should().Be("1"); // Indexed value
    }

    [Fact]
    public async Task CanAutoRefreshOnRead()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionBeforeReading());
        var prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1);
        prj.Id.Should().Be(Wrapper.StreamId1);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);
    }

    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
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
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
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
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>().ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanListWithFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "1").ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanFindNothingWithBadFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "2").ToList();
        projections.Count().Should().Be(0);
    }

    [Fact]
    public async Task CanManuallyRefresh()
    {
        using var wrapper = new Wrapper();
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(0);
        await wrapper.Sut.TryRefreshProjections(Wrapper.StreamId1);
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }

    [Fact]
    public async Task CanAutomaticallyRefresh()
    {
        using var wrapper = new Wrapper(cfg=>cfg.RefreshProjectionsEvery(TimeSpan.FromSeconds(1)));
        await Task.Delay(TimeSpan.FromSeconds(1.1));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }
}