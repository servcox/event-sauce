using Azure;
using Azure.Data.Tables;
using FluentAssertions;
using ServcoX.EventSauce.Tests.V2.TestData;
using ServcoX.EventSauce.V2.TableRecords;
using ServcoX.EventSauce.V2.Utilities;

namespace ServcoX.EventSauce.Tests.V2;

public class EventStoreProjectionRefreshTests
{
    
    [Fact]
    public void ProjectionsNotRefreshedUnlessAsked()
    {
        using var wrapper = new Wrapper();
        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);
        Assert.Throws<RequestFailedException>(() => wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId1));
    }
    
    
    [Fact]
    public async Task CanManuallyRefresh()
    {
        using var wrapper = new Wrapper();
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(0);
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }

    [Fact]
    public async Task CanRefreshOnTimer()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionsEvery(TimeSpan.FromSeconds(1)));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(0);
        await Task.Delay(TimeSpan.FromSeconds(1.1));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }
    
    [Fact]
    public async Task CanRefreshOnStartup()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionsOnStartup());
        var maxTimestamp = await wrapper.Sut.RefreshAllProjections();
        maxTimestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }
    
    [Fact]
    public async Task CanManuallyRefreshAll()
    {
        using var wrapper = new Wrapper();
        var maxTimestamp = await wrapper.Sut.RefreshAllProjections();
        maxTimestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }
    

    [Fact]
    public async Task CanRefreshSpecific()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjections(Wrapper.StreamId1);

        // Check that the index was created
        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);
        var record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId1).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(3);
        record.GetString("A").Should().Be("1");
    }

    [Fact]
    public async Task CanRefreshAllSince()
    {
        using var wrapper = new Wrapper();
        var maxTimestamp = await wrapper.Sut.RefreshAllProjections(DateTime.UtcNow.AddSeconds(-10));
        maxTimestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(1));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }

    [Fact]
    public async Task CanNotRefreshAllInFuture()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshAllProjections(DateTime.UtcNow.AddSeconds(10));
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(0);
    }

    [Fact]
    public async Task CanRefreshOnWrite()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionsAfterWriting());
        await wrapper.Sut.WriteEvents(Wrapper.StreamId2, new TestAEvent("a"), Wrapper.UserId);

        // Check that the index was created
        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);
        var record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId2).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(1);
        record.GetString("A").Should().Be("1");
    }

    [Fact]
    public async Task CanRefreshOnRead()
    {
        using var wrapper = new Wrapper(cfg => cfg.RefreshProjectionBeforeReading());
        var prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1);
        prj.Id.Should().Be(Wrapper.StreamId1);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);
    }
}