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
    public async Task CanReadWithPartial()
    {
        using var wrapper = new EventStoreWrapper();
        wrapper.WriteSegment(TestData.AtDate, 0, TestData.A1Raw);
        wrapper.WriteSegment(TestData.AtDate, 0, $"{TestData.At:yyyyMMdd\\THHmmssK}TEST."); // <== This is a incomplete event that doesn't end with a newline

        var actual = await wrapper.Sut.Read();
        actual.Should().BeEquivalentTo(new[] { new Record(TestData.At, new(TestData.A1.GetType()), TestData.A1) });
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
        var receivedCount = 0;
        using var wrapper = new EventStoreWrapper(cfg => cfg
            .PollEvery(TimeSpan.FromSeconds(0.5)) // <== NOTE
            .OnEvent<TestData.TestEventA>((_, metadata) =>
            {
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventA));
                receivedCount++;
            })
        );

        const Int32 factor = 100;
        var sentCount = 0;
        for (var i = 0; i < factor; i++) await wrapper.Sut.Write(new TestData.TestEventA { A = sentCount++.ToString() }, TestData.At);

        await Task.Delay(1000);

        receivedCount.Should().Be(sentCount);

        for (var i = 0; i < factor; i++) await wrapper.Sut.Write(new TestData.TestEventA { A = sentCount++.ToString() }, TestData.At);

        await Task.Delay(1000);

        receivedCount.Should().Be(sentCount);
    }

    [Fact]
    public async Task CanPollNow()
    {
        var receivedCount = 0;
        using var wrapper = new EventStoreWrapper(cfg => cfg
            .OnEvent<TestData.TestEventA>((_, metadata) =>
            {
                metadata.At.Should().Be(TestData.At);
                metadata.Type.TryDecode().Should().Be(typeof(TestData.TestEventA));
                receivedCount++;
            })
        );

        const Int32 factor = 100;
        var sentCount = 0;
        for (var i = 0; i < factor; i++) await wrapper.Sut.Write(new TestData.TestEventA { A = sentCount++.ToString() }, TestData.At);

        await wrapper.Sut.PollNow();

        receivedCount.Should().Be(sentCount);

        for (var i = 0; i < factor; i++) await wrapper.Sut.Write(new TestData.TestEventA { A = sentCount++.ToString() }, TestData.At);

        await wrapper.Sut.PollNow();

        receivedCount.Should().Be(sentCount);
    }

    [Fact]
    public async Task CanWriteReadMultiThread()
    {
        var containerName = $"unittest-{DateTime.Now:yyyyMMddHHmmss}-{Guid.NewGuid():N}";
        using var wrapper1 = new EventStoreWrapper(containerName: containerName);
        using var wrapper2 = new EventStoreWrapper(containerName: containerName);
        await wrapper1.Sut.Write(TestData.A1, TestData.At);
        await wrapper1.Sut.Write(TestData.A2, TestData.At);
        await wrapper1.Sut.Write(TestData.B, TestData.At);

        await wrapper2.Sut.PollNow();

        var expected = new Object[]
        {
            TestData.A1,
            TestData.A2,
            TestData.B,
        }.Select(a => new Record(TestData.At, new(a.GetType()), a));
        var actual = await wrapper2.Sut.Read();
        actual.Should().BeEquivalentTo(expected);
    }
}