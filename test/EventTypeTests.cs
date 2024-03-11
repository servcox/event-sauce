using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public class EventTypeTests
{
    private readonly Type _type = typeof(Test);

    [Fact]
    public void CanEncode() => new EventType(_type).Name.Should().Be(_type.FullName!.ToUpperInvariant());

    [Fact]
    public void CanDecode() => new EventType(_type.FullName!.ToUpperInvariant()).TryDecode().Should().Be(_type);

    private class Test;
}