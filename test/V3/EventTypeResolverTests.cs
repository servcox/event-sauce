using FluentAssertions;

namespace ServcoX.EventSauce.Tests.V3;

public class EventTypeResolverTests
{
    private readonly EventTypeResolver _sut = new();
    private readonly Type _type = typeof(Test);

    [Fact]
    public void CanEncode() => _sut.Encode(_type).Should().Be(_type.FullName!.ToUpperInvariant());

    [Fact]
    public void CanDecode() => _sut.TryDecode(_type.FullName!.ToUpperInvariant()).Should().Be(_type);

    private class Test;
}