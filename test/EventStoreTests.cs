namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    private const Int32 AzureBlockLimit = 50_000;

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
    public async Task CanWriteOverflowSLOW()
    {
        using var wrapper = new EventStoreWrapper();
        wrapper.WriteEmptyRecords(TestData.AtDate, 0, AzureBlockLimit - 1);
        await wrapper.Sut.Write(TestData.A1, TestData.At);
        await wrapper.Sut.Write(TestData.A2, TestData.At);
        await wrapper.Sut.Write(TestData.B, TestData.At);
        wrapper.AssertSegment(TestData.AtDate, 0, new String('\n', AzureBlockLimit - 1) + TestData.A1Raw);
        wrapper.AssertSegment(TestData.AtDate, 1, TestData.A2Raw + TestData.BRaw);
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