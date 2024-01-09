using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public class ProjectionIdTests
{
    [Theory]
    [InlineData(typeof(ClassA1), 0, "ServcoX.EventSauce.Tests.ClassA1@0.kSmuLONJd-4")]
    [InlineData(typeof(ClassA1), 1, "ServcoX.EventSauce.Tests.ClassA1@1.kSmuLONJd-4")]
    [InlineData(typeof(ClassA2), 0, "ServcoX.EventSauce.Tests.ClassA2@0.635BK8CCgnA")]
    [InlineData(typeof(ClassB), 0, "ServcoX.EventSauce.Tests.ClassB@0.9wEBXo7UJUg")]
    public void CanCompute(Type type, UInt32 version, String expected) => ProjectionId.Compute(type, version).Should().Be(expected);
}

public class ClassA1
{
    public Int32 A { get; set; }
}

public class ClassA2
{
    public Int32 A { get; set; }
}

public class ClassB
{
    public Int32 B { get; set; }
}