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
    public async Task CanNotReadMissing()
    {
        using var wrapper = new ProjectionWrapper();
        await Assert.ThrowsAsync<NotFoundException>(() => wrapper.Sut.Read<Cake>("bad"));
    }

    [Fact]
    public async Task CanTryRead()
    {
        using var wrapper = new ProjectionWrapper();
        await wrapper.PopulateTestData();
        var projection = await wrapper.Sut.TryRead<Cake>(wrapper.AggregateId1);
        wrapper.Assert1(projection!);
    }

    [Fact]
    public async Task CanTryReadMissing()
    {
        using var wrapper = new ProjectionWrapper();
        var projection = await wrapper.Sut.TryRead<Cake>("bad");
        projection.Should().BeNull();
    }

    [Fact]
    public async Task CanList()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        var projections = await wrapper.Sut.List<Cake>();
        projections.Count.Should().Be(2);
        wrapper.Assert1(projections.Single(projection => projection.Id == wrapper.AggregateId1));
        wrapper.Assert2(projections.Single(projection => projection.Id == wrapper.AggregateId2));
    }

    [Fact]
    public async Task CanQueryByString()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        var projections = await wrapper.Sut.Query<Cake>(nameof(Cake.Color), "GREEN");
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanQueryByNumber()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        var projections = await wrapper.Sut.Query<Cake>(nameof(Cake.Slices), 1);
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanMultiFacetQuery()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        var projections = await wrapper.Sut.Query<Cake>(new Dictionary<String, Object>
        {
            [nameof(Cake.Color)] = "GREEN",
            [nameof(Cake.Slices)] = 1,
        });
        projections.Count.Should().Be(1);
        wrapper.Assert2(projections[0]);
    }

    [Fact]
    public async Task CanFindNothingWithBadQuery()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        var projections = await wrapper.Sut.Query<Cake>(nameof(Cake.Color), "BANANANA");
        projections.Count.Should().Be(0);
    }

    [Fact]
    public async Task CanNotQueryWithoutIndex()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        await Assert.ThrowsAsync<MissingIndexException>(() => wrapper.Sut.Query<Cake>(nameof(Cake.Id), "BANANANA"));
    }

    [Fact]
    public async Task CanAutoSync()
    {
        using var wrapper = new ProjectionWrapper(prePopulate: true);
        await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        var projection = await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    [Fact]
    public async Task CanElectNotToSync()
    {
        using var wrapper = new ProjectionWrapper(cfg => cfg.DoNotSyncBeforeReads(), prePopulate: true);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        var projection = await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        projection.Color.Should().Be("BLUE");
    }

    [Fact]
    public async Task CanManualSync()
    {
        using var wrapper = new ProjectionWrapper(cfg => cfg.DoNotSyncBeforeReads(), prePopulate: true);
        await wrapper.EventStore.WriteEvent(wrapper.AggregateId1, new CakeIced { Color = "BLACK" });
        await wrapper.Sut.Sync();
        var projection = await wrapper.Sut.Read<Cake>(wrapper.AggregateId1);
        projection.Color.Should().Be("BLACK");
    }

    [Fact]
    public async Task CanLoadRemoteCache()
    {
        using var wrapper = new ProjectionWrapper();
        var blob = wrapper.GetBlobClient();
        await using var stream = File.OpenRead("TestData/ServcoX.EventSauce.Tests.TestData.Cake@1.brlJHUCgB1E.bois.lz4");
        await blob.UploadAsync(stream);
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

    [Fact]
    public void CanNotUseInvalidIndexName() => Assert.Throws<InvalidIndexNameException>(() =>
    {
        new ProjectionStore(null!, cfg => cfg
            .DefineProjection<Cake>(1, builder => builder
                .IndexField("bad")
            ));
    });
}