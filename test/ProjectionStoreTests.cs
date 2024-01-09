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
        throw new NotImplementedException();
    }
    
    
    [Fact]
    public async Task CanLoadRemoteCache()
    {
        throw new NotImplementedException();
    }
    
    [Fact]
    public async Task CanWriteRemoteCache()
    {
        throw new NotImplementedException();
    }
    
    [Fact]
    public async Task CanUpdateRemoteCache()
    {
        throw new NotImplementedException();
    }
}