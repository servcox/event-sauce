using System.Text;
using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public sealed class EventStreamTests
{
    private static readonly IEvent[] Decoded = [TestData.A1, TestData.A2, TestData.B];
    private static readonly String Encoded = TestData.A1Raw + TestData.A2Raw + TestData.BRaw;

    [Fact]
    public void CanEncode()
    {
        EventType.Register<TestData.TestEventA>();
        using var stream = EventStream.Encode(Decoded, TestData.At);
        stream.ReadAllAsUtf8().Should().Be(Encoded);
    }

    [Fact]
    public void CanDecode()
    {
        EventType.Register<TestData.TestEventA>();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(Encoded));
        var expected = Decoded.Select(i => new Record(TestData.At, new(i.GetType()), i)).ToList();
        var (length, records) = EventStream.Decode(stream);
        length.Should().Be(Encoded.Length);
        records.Should().BeEquivalentTo(expected);
    }
}