using FluentAssertions;
using ServcoX.EventSauce.Tests.Fixtures;

namespace ServcoX.EventSauce.Tests;

public class ProjectionStoreTests
{
    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projection = await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        wrapper.Assert1(projection);
    }

    [Fact]
    public async Task CanList()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projections = await wrapper.Sut.List<Cake>();
        projections.Count.Should().Be(2);
        wrapper.Assert1(projections[0]);
        wrapper.Assert2(projections[1]);
    }

    [Fact]
    public async Task CanQuery()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projections = await wrapper.Sut.Query<Cake>(nameof(Cake.Color), "GREEN");
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanReadLateEvent()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        var projection = await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    // TODO: Test all types of syncinc

    [Fact]
    public async Task CanLoadRemoteCache()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanWriteRemoteCache()
    {
        using var wrapper = new ProjectionWrapper(cfg => cfg.WriteRemoteCacheEvery(TimeSpan.FromSeconds(1)), prePopulate: true);
        await Task.Delay(1500);
        var exists = await wrapper.GetBlobClient().ExistsAsync();
        exists.Value.Should().BeTrue();
    }
}