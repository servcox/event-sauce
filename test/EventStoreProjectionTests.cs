using System.Text.Json;
using FluentAssertions;
using ServcoX.EventSauce.TableRecords;
using ServcoX.EventSauce.Tests.TestData;
using ServcoX.EventSauce.Utilities;

namespace ServcoX.EventSauce.Tests;

public class EventStoreProjectionTests
{
    [Fact]
    public async Task CanReadProjection()
    {
        using var wrapper = new Wrapper();

        var prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1, CancellationToken.None);
        prj.A.Should().Be(1);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(3);
        prj.Other.Should().Be(1);

        var projectionId = ProjectionIdUtilities.Compute(typeof(TestProjection), Wrapper.ProjectionVersion);

        var record = wrapper.ProjectionTable.GetEntity<ProjectionRecord>(projectionId, Wrapper.StreamId1).Value;
        record.Version.Should().Be(3);
        record.Body.Should().Be(JsonSerializer.Serialize(prj));

        await wrapper.Sut.WriteStream(Wrapper.StreamId1, new TestAEvent("a"), Wrapper.UserId, CancellationToken.None);

        prj = await wrapper.Sut.ReadProjection<TestProjection>(Wrapper.StreamId1, CancellationToken.None);
        prj.A.Should().Be(2);
        prj.B.Should().Be(1);
        prj.Any.Should().Be(4);
        prj.Other.Should().Be(1);

        record = wrapper.ProjectionTable.GetEntity<ProjectionRecord>(projectionId, Wrapper.StreamId1).Value;
        record.Version.Should().Be(4);
        record.Body.Should().Be(JsonSerializer.Serialize(prj));
    }
}