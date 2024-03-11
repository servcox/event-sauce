using System.Text;
using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public sealed class EventStreamTests
{
    private static readonly DateTime At = new(2000, 1, 1);

    private static readonly IEvent[] Decoded =
    [
        TestEvents.A1,
        TestEvents.A2
    ];

    private static readonly String Encoded =
        "20000101T000000\tSERVCOX.EVENTSAUCE.TESTS.TESTEVENTS+TESTEVENT\t{\"A\":\"a1\"}\n" +
        "20000101T000000\tSERVCOX.EVENTSAUCE.TESTS.TESTEVENTS+TESTEVENT\t{\"A\":\"a2\"}\n";

    [Fact]
    public void CanEncode()
    {
        EventType.Register<TestEvents.TestEvent>();
        using var stream = EventStream.Encode(Decoded, At);
        stream.ReadAllAsUtf8().Should().Be(Encoded);
    }

    [Fact]
    public void CanDecode()
    {
        EventType.Register<TestEvents.TestEvent>();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(Encoded));
        var expected = Decoded.Select(i => new Record(At, new(typeof(ServcoX.EventSauce.Tests.TestEvents.TestEvent)), i)).ToList();
        EventStream.Decode(stream).Should().BeEquivalentTo(expected);
    }
}