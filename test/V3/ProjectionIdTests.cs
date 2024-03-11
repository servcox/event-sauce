using FluentAssertions;

namespace ServcoX.EventSauce.Tests.V3;

public class ProjectionIdTests
{
    [Theory]
    [InlineData(typeof(ClassA1), 0, "ServcoX.EventSauce.Tests.V3.ClassA1@0.TaHmbNcKSPI")]
    [InlineData(typeof(ClassA1), 1, "ServcoX.EventSauce.Tests.V3.ClassA1@1.TaHmbNcKSPI")]
    [InlineData(typeof(ClassA2), 0, "ServcoX.EventSauce.Tests.V3.ClassA2@0.biXmcH1v5us")]
    [InlineData(typeof(ClassB), 0, "ServcoX.EventSauce.Tests.V3.ClassB@0.cO1-Nj1o-vI")]
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