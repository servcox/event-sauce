namespace ServcoX.EventSauce.Tests;

public class EventStoreTests
{
    [Fact]
    public async Task CanWrite()
    {
      
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanWriteMultiple()
    {
        throw new NotImplementedException();
    }
    
    [Fact]
    public async Task CanWriteOverflow()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task CanWriteNil()
    {
        using var wrapper = new EventStoreWrapper();
        await wrapper.Sut.Write([]);
        // TODO: Check nothing was written
        
        throw new NotImplementedException();
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