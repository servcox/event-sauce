using ServcoX.EventSauce.EventStores;

// Connect to the event store
const String connectionString = "UseDevelopmentStorage=true;";
var eventStore = new EventStore(connectionString);

// Write a bunch of events to a stream
const String streamType = "CAKE";
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await eventStore.CreateStream(streamId, streamType, CancellationToken.None);
await eventStore.WriteStream(streamId, new BakedCake(), userId, CancellationToken.None);
await eventStore.WriteStream(streamId, new IcedCake("BLUE"), userId, CancellationToken.None);
await eventStore.WriteStream(streamId, new CutCake(3), userId, CancellationToken.None);

// List streams
foreach (var stream in eventStore.ListStreams(streamType))
{
    Console.WriteLine(stream.Id);
}

// Read events from stream
foreach (var evt in eventStore.ReadStream(streamId, 0)) // <== Can pick a greater version to only read new events
{
    Console.WriteLine(evt.Version + ": " + evt.Body);
}

// Define your events
public readonly record struct BakedCake : IEventBody;

public readonly record struct IcedCake(String Color) : IEventBody;

public readonly record struct CutCake(Int32 Slices) : IEventBody;