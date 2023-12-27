using System.Text.Json;
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
    public void CanNotUseReservedIndexName() => Assert.Throws<InvalidIndexName>(() =>
    {
        new EventStore(Wrapper.DevelopmentConnectionString, cfg => cfg
            .DefineProjection<TestProjection>(Wrapper.StreamType1, Wrapper.ProjectionVersion, builder => builder
                .Index(nameof(ProjectionRecord.ProjectionId), prj => prj.A.ToString())
            ));
    });

    [Fact]
    public void CanNotUseTooLongIndexName() => Assert.Throws<InvalidIndexName>(() =>
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

        var prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1, CancellationToken.None);
        prj.Id.Should().Be(Wrapper.StreamId1);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);

        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);

        var record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId1).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(3);
        record.GetString(nameof(ProjectionRecord.Body)).Should().Be(JsonSerializer.Serialize(prj));
        record.GetString("A").Should().Be("1"); // Indexed value

        await wrapper.Sut.WriteEvents(Wrapper.StreamId1, new TestAEvent("a"), Wrapper.UserId, CancellationToken.None);

        prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1, CancellationToken.None);
        prj.Id.Should().Be(Wrapper.StreamId1);
        prj.A.Should().Be(2);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(4);
        prj.Other.Should().Be(1);

        record = wrapper.ProjectionTable.GetEntity<TableEntity>(projectionId, Wrapper.StreamId1).Value;
        record.GetInt64(nameof(ProjectionRecord.Version)).Should().Be(4);
        record.GetString(nameof(ProjectionRecord.Body)).Should().Be(JsonSerializer.Serialize(prj));
        record.GetString("A").Should().Be("2");
    }

    [Fact]
    public async Task CanList()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjection<TestProjection>();
        var projections = wrapper.Sut.ListProjections<TestProjection>().ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }

    [Fact]
    public async Task CanListWithFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjection<TestProjection>();
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "1").ToList();
        projections.Count.Should().Be(1);
        projections.Should().ContainSingle(projection => projection.Id == Wrapper.StreamId1);
    }
    
    [Fact]
    public async Task CanFindNothingWithBadFilter()
    {
        using var wrapper = new Wrapper();
        await wrapper.Sut.RefreshProjection<TestProjection>();
        var projections = wrapper.Sut.ListProjections<TestProjection>(nameof(TestProjection.A), "2").ToList();
        projections.Count().Should().Be(0);
    }

    [Fact]
    public async Task CanRefreshProjection()
    {
        using var wrapper = new Wrapper();
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(0);
        await wrapper.Sut.RefreshProjection<TestProjection>();
        wrapper.Sut.ListProjections<TestProjection>().Count().Should().Be(1);
    }
}