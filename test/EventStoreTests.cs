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
    public async Task CanReadAll()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanReadSince()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanAutoPollEvents()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanManuallyPollEvents()
    {
        throw new NotImplementedException();
    }
}