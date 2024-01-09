using FluentAssertions;
using ServcoX.EventSauce.V2.Utilities;

namespace ServcoX.EventSauce.Tests.V2;

public class ProjectionIdTests
{
    [Theory]
    [InlineData(typeof(ClassA1), 0, "ServcoX.EventSauce.Tests.V2.ClassA1@0.5hL8zAhJQWw")]
    [InlineData(typeof(ClassA1), 1, "ServcoX.EventSauce.Tests.V2.ClassA1@1.5hL8zAhJQWw")]
    [InlineData(typeof(ClassA2), 0, "ServcoX.EventSauce.Tests.V2.ClassA2@0.tyoytf0T9hI")]
    [InlineData(typeof(ClassB), 0, "ServcoX.EventSauce.Tests.V2.ClassB@0.LWd8Vot-Aco")]
    public void CanComputeId(Type type, UInt32 version, String expected)
    {
        EventSauce.ProjectionId.Compute(type, version).Should().Be(expected);
    }
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