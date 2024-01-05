namespace ServcoX.EventSauce;

public class EventStore
{
    public EventStore(String topic, String connectionString)
    {
        throw new NotImplementedException();
    }
    
    public async Task Write(Event evt) => Write(new[] { evt });
    public async Task Write(IEnumerable<Event> events) => Write(events.ToArray());
    public async Task Write(Event[] events) // TODO: CreatedBy
    {
        throw new NotImplementedException();
    }

    public async Task<Event[]> Read(UInt64 offset, String? streamId = null)
    {
        throw new NotImplementedException();
    }
}