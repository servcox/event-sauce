using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public class GenericActionTests
{
    [Fact]
    public void CanInvoke()
    {
        var hit = 0;
        var sut = new GenericAction(() => hit++);
        sut.Invoke();
        hit.Should().Be(1);
    }
}