using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public async Task CanWrite()
    {
        using var wrapper = new EventStoreWrapper();
        await wrapper.Sut.Write(TestData.A1, TestData.At);
        await wrapper.Sut.Write(TestData.A2, TestData.At);
        await wrapper.Sut.Write(TestData.B, TestData.At);
        wrapper.AssertSegment(TestData.AtDate, 0, TestData.A1Raw + TestData.A2Raw + TestData.BRaw);
    }

    [Fact]
    public async Task CanWriteMultiple()
    {
        using var wrapper = new EventStoreWrapper();
        await wrapper.Sut.Write(new IEvent[]
        {
            TestData.A1,
            TestData.A2,
            TestData.B
        }, TestData.At);
        wrapper.AssertSegment(TestData.AtDate, 0, TestData.A1Raw + TestData.A2Raw + TestData.BRaw);
    }

    [Fact]
    public async Task CanWriteNone()
    {
        using var wrapper = new EventStoreWrapper();
        await wrapper.Sut.Write([], TestData.At);

        wrapper.AssertSegment(TestData.AtDate, 0, String.Empty);
    }

    [Fact]
    public async Task CanWriteOverflow()
    {
        using var wrapper = new EventStoreWrapper();
        for (var i = 0; i < EventStoreWrapper.TargetWritesPerSegment - 1; i++) await wrapper.Sut.Write(TestData.A1, TestData.At);
        await wrapper.Sut.Write(TestData.A2, TestData.At);
        await wrapper.Sut.Write(TestData.B, TestData.At);

        wrapper.AssertSegment(TestData.AtDate, 0, String.Concat(Enumerable.Repeat(TestData.A1Raw, EventStoreWrapper.TargetWritesPerSegment - 1)) + TestData.A2Raw);
        wrapper.AssertSegment(TestData.AtDate, 1, TestData.BRaw);
    }
    
    [Fact]
    public async Task CanRead()
    {
        using var wrapper = new EventStoreWrapper();
        wrapper.WriteSegment(TestData.AtDate, 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate, 0, TestData.A2Raw);
        wrapper.WriteSegment(TestData.AtDate, 1, TestData.BRaw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 0, TestData.A2Raw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 1, TestData.BRaw);

        var expected = new Object[]
        {
            TestData.A1,
            TestData.A2,
            TestData.B,
            TestData.A1,
            TestData.A2,
            TestData.B,
        }.Select(a => new Record(TestData.At, new(a.GetType()), a));
        
        var actual = await wrapper.Sut.Read();
        actual.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task CanReadSince()
    {
        using var wrapper = new EventStoreWrapper();
        wrapper.WriteSegment(TestData.AtDate, 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate, 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate, 1, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 0, TestData.A2Raw);
        wrapper.WriteSegment(TestData.AtDate.AddDays(1), 1, TestData.BRaw);

        var expected = new Object[]
        {
            TestData.A1,
            TestData.A2,
            TestData.B,
        }.Select(a => new Record(TestData.At, new(a.GetType()), a));
        
        var actual = await wrapper.Sut.Read(TestData.AtDate.AddDays(1));
        actual.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task CanAutoPollEvents()
    {
        var eventACount = 0;
        var eventBCount = 0;
        var anyEventCount = 0;
        using var wrapper = new EventStoreWrapper(cfg => cfg
            .PollEvery(TimeSpan.FromSeconds(1)) // <== NOTE
            .OnEvent<TestData.TestEventA>((evt,metadata) =>
            {
                evt.A.Should().BeOneOf(TestData.A1.A, TestData.A2.A);
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventA));
                eventACount++;
            })
            .OnOtherEvent((evt, metadata) =>
            {
                evt.Should().BeEquivalentTo(TestData.B);
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventB));
                eventBCount++;
            })
            .OnAnyEvent((evt, metadata) =>
            {
                metadata.At.Should().Be(TestData.At);
                anyEventCount++;
            })
        );
        await wrapper.Sut.Write(TestData.A1, TestData.At);
        await wrapper.Sut.Write(TestData.A2, TestData.At);
        await wrapper.Sut.Write(TestData.B, TestData.At);
        await Task.Delay(2000);
        
        eventACount.Should().Be(2);
        eventBCount.Should().Be(1);
        anyEventCount.Should().Be(3);
    }

    [Fact]
    public async Task CanManuallyPollEvents()
    {
        var eventACount = 0;
        var eventBCount = 0;
        var anyEventCount = 0;
        using var wrapper = new EventStoreWrapper(cfg => cfg
            .OnEvent<TestData.TestEventA>((evt,metadata) =>
            {
                evt.A.Should().BeOneOf(TestData.A1.A, TestData.A2.A);
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventA));
                eventACount++;
            })
            .OnOtherEvent((evt, metadata) =>
            {
                evt.Should().BeEquivalentTo(TestData.B);
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventB));
                eventBCount++;
            })
            .OnAnyEvent((evt, metadata) =>
            {
                metadata.At.Should().Be(TestData.At);
                anyEventCount++;
            })
        );
        await wrapper.Sut.Write(TestData.A1, TestData.At);
        await wrapper.Sut.Write(TestData.A2, TestData.At);
        await wrapper.Sut.Write(TestData.B, TestData.At);
        await wrapper.Sut.PollNow();
        
        eventACount.Should().Be(2);
        eventBCount.Should().Be(1);
        anyEventCount.Should().Be(3);
    }
}