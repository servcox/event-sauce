using FluentAssertions;
using ServcoX.EventSauce.TableRecords;

namespace ServcoX.EventSauce.Tests;

public class IndexRecordTests
{
    private const String ProjectionId = "PROJECTIONID";
    private const String Field = "FIELD";
    private const String Value = "VALUE";
    private const String PartitionKey = $"{ProjectionId}/{Field}/{Value}";

    [Fact]
    public void CanEncodePartitionKey()
    {
        var sut = new IndexRecord { ProjectionId = ProjectionId, Field = Field, Value = Value, };
        sut.PartitionKey.Should().Be(PartitionKey);
    }
    
    [Fact]
    public void CanDecodePartitionKey()
    {
        var sut = new IndexRecord { PartitionKey = PartitionKey };
        sut.ProjectionId.Should().Be(ProjectionId);
        sut.Field.Should().Be(Field);
        sut.Value.Should().Be(Value);
    }
}