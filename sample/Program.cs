using ServcoX.EventSauce.EventStores;

// Connect to the event store
const String connectionString = "UseDevelopmentStorage=true;";
var store = new EventStore(connectionString);

// Write a bunch of events to a stream
const String streamType = "CAKE";
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await store.CreateStream(streamId, streamType, CancellationToken.None);
await store.WriteStream(streamId, new BakedCake(), userId, CancellationToken.None);
await store.WriteStream(streamId, new CakeIced("BLUE"), userId, CancellationToken.None);
await store.WriteStream(streamId, new CakeCut(3), userId, CancellationToken.None);

// List streams
foreach (var stream in store.ListStreams(streamType))
{
    Console.WriteLine(stream.Id);
}

// Read events from stream
foreach (var evt in store.ReadStream(streamId, 0)) // <== Can pick a greater version to only read new events
{
    Console.WriteLine(evt.Version + ": " + evt.Body);
}

store.CreateProjection(streamType, () => new CakeProjector());

store.QueryProjection<Cake>(a => a.Color == "BLUE");

store.FindProjection<Cake>(streamId);

// Define projection
public readonly record struct Cake
{
    public String Color { get; init; }
}

// Define projector
public class CakeProjector : IProjector<Cake>
{
    private Cake _subject;

    public void New()
    {
        _subject = new();
    }

    public void Load(String streamId, Cake projection)
    {
        _subject = projection;
    }

    public Cake Unload()
    {
        return _subject;
    }

    public void Apply(CakeIced body, Event evt)
    {
        _subject = _subject with
        {
            Color = body.Color,
        };
    }
    
    public void PromiscuousApply(Event evt)
    {
    }

    public void FallbackApply(Event evt)
    {
    }
}

// Define events
public readonly record struct BakedCake : IEventBody;

public readonly record struct CakeIced(String Color) : IEventBody;

public readonly record struct CakeCut(Int32 Slices) : IEventBody;

