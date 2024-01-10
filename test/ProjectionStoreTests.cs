using FluentAssertions;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests;

public class ProjectionStoreTests
{
    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projection = await wrapper.Sut.Read(wrapper.AggregateId1);
        wrapper.Assert1(projection);
    }

    [Fact]
    public async Task CanNotReadMissing()
    {
        using var wrapper = new ProjectionWrapper();
        await Assert.ThrowsAsync<NotFoundException>(() => wrapper.Sut.Read("bad"));
    }

    [Fact]
    public async Task CanTryRead()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projection = await wrapper.Sut.TryRead(wrapper.AggregateId1);
        wrapper.Assert1(projection!);
    }

    [Fact]
    public async Task CanTryReadMissing()
    {
        using var wrapper = new ProjectionWrapper();
        var projection = await wrapper.Sut.TryRead("bad");
        projection.Should().BeNull();
    }

    [Fact]
    public async Task CanList()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        var projections = await wrapper.Sut.List();
        projections.Count.Should().Be(2);
        wrapper.Assert1(projections.Single(projection => projection.Id == wrapper.AggregateId1));
        wrapper.Assert2(projections.Single(projection => projection.Id == wrapper.AggregateId2));
    }

    [Fact]
    public async Task CanQueryByString()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        var projections = await wrapper.Sut.Query(nameof(Cake.Color), "GREEN");
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanQueryByNumber()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        var projections = await wrapper.Sut.Query(nameof(Cake.Slices), "1");
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanMultiFacetQuery()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        var projections = await wrapper.Sut.Query(new Dictionary<String, String>
        {
            [nameof(Cake.Color)] = "GREEN",
            [nameof(Cake.Slices)] = "1",
        });
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanFindNothingWithBadQuery()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        var projections = await wrapper.Sut.Query(nameof(Cake.Color), "BANANANA");
        projections.Count.Should().Be(0);
    }

    [Fact]
    public async Task CanNotQueryWithoutIndex()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        await Assert.ThrowsAsync<MissingIndexException>(() => wrapper.Sut.Query(nameof(Cake.Id), "BANANANA"));
    }

    [Fact]
    public async Task CanAutoSync()
    {
        using var wrapper = new ProjectionWrapper(prePopulateData: true);
        await wrapper.Sut.Read(wrapper.AggregateId1);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        var projection = await wrapper.Sut.Read(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    [Fact]
    public async Task CanElectNotToSync()
    {
        using var wrapper = new ProjectionWrapper(storeBuilder: cfg => cfg.DoNotSyncBeforeReads(), prePopulateData: true);
        await Assert.ThrowsAsync<NotFoundException>(() => wrapper.Sut.Read(wrapper.AggregateId1));
    }

    [Fact]
    public async Task CanManualSync()
    {
        using var wrapper = new ProjectionWrapper(storeBuilder: cfg => cfg.DoNotSyncBeforeReads(), prePopulateData: true);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        await wrapper.EventStore.Sync();
        var projection = await wrapper.Sut.Read(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    [Fact]
    public async Task CanSyncOnTimer()
    {
        using var wrapper = new ProjectionWrapper(storeBuilder: cfg => cfg.DoNotSyncBeforeReads().SyncEvery(TimeSpan.FromSeconds(1)), prePopulateData: true);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        await Task.Delay(1500);
        var projection = await wrapper.Sut.Read(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    [Fact]
    public void CanNotUseInvalidIndexName()
    {
        var cfg = new ProjectionConfiguration<Cake>();
        Assert.Throws<InvalidIndexNameException>(() => cfg.IndexField("bad"));
    }
}