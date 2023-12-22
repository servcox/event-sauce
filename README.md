# ServcoX.EventSauce
Event Sauce is a super light-weight event sourcing library we use internally to store our events in Azure Table Storage. Its for when you want to use event sourcing but your needs aren't demanding or your budget is modest. We use it for internal projects with a few hundred user and it works a treat. It's not Kafka, and it's not trying to be. 

Because it backs onto Azure Table Storage, it's cost is tiny compared to other options. It's also simple to allow you to build on it in your own way.

# TLDR;

Define your events like this:
```c#
public readonly record struct BakedCake : IEventBody;
public readonly record struct IcedCake(String Color) : IEventBody;
public readonly record struct CutCake(Int32 Slices) : IEventBody;
```

Connect to your event store like this:
```c#
const String connectionString = "UseDevelopmentStorage=true;"; // Your Azure Storage connection string goes here
var eventStore = new EventStore(connectionString);
```

Create a stream and write events like this:
```c#
const String streamType = "CAKE";
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await eventStore.CreateStream(streamId, streamType, CancellationToken.None);
await eventStore.WriteStream(streamId, new BakedCake(), userId, CancellationToken.None);
await eventStore.WriteStream(streamId, new IcedCake("BLUE"), userId, CancellationToken.None);
await eventStore.WriteStream(streamId, new CutCake(3), userId, CancellationToken.None);
```

Get a list of streams you've already created like so:
```c#
foreach (var stream in eventStore.ListStreams(streamType))
{
    Console.WriteLine(stream.Id);
}
```

And finally, read events back like here:
```c#
foreach (var evt in eventStore.ReadStream(streamId, 0)) // <== Can pick a greater version to only read new events
{
    Console.WriteLine(evt.Version + ": " + evt.Body);
}
```

Simples!