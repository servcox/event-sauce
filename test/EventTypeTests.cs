using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public sealed class EventTypeTests
{
    private readonly Type _type = typeof(Test);

    [Fact]
    public void CanEncode() => new EventType(_type).Name.Should().Be(_type.FullName!.ToUpperInvariant());

    [Fact]
    public void CanDecode()
    {
        EventType.Register(_type);
        new EventType(_type.FullName!.ToUpperInvariant()).TryDecode().Should().Be(_type);
    }

    [Fact]
    public void CanDecodeAsNullWhenNotRegistered() => new EventType("bad").TryDecode().Should().BeNull();

    private class Test;
}