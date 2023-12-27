using FluentAssertions;
using ServcoX.EventSauce.Extensions;

namespace ServcoX.EventSauce.Tests;

public class StringExtensionTests
{
    [Theory]
    [InlineData(@"a\b\c", 1, 1)]
    [InlineData(@"a\b\c", 2, 3)]
    [InlineData(@"a\b\c", 3, -1)]
    [InlineData(@"aa\bb\cc", 1, 2)]
    [InlineData(@"aa\bb\cc", 2, 5)]
    [InlineData(@"aa\bb\cc", 3, -1)]
    public void CanGetNthIndexOf(String target, Int32 nth, Int32 expected) => target.IndexOfNth('\\', nth).Should().Be(expected);
}