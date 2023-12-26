# ServcoX.EventSauce
Event Sauce is the light-weight event sourcing library Servco uses internally to store our events in Azure Table Storage. 
Its for when you want to use event sourcing but your needs (or budget) aren't demanding enough to justify using Kafka. 

It's performant a modest scale, and since it backs on Azure Table Storage it's cost is tiny compared to just about 
everything else. It's also simple, and allows you to build out event sourcing in a way that suits you.

# Basic usage

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

# Projections
Once you have events being stored, you can then go a step futher and create projections based on them.

Create a projection like this:
```c#
public record Cake
{
    public Int32 Slices { get; set; }
    public String Color { get; set; }
}
```

When you're creating your store, define how to build the projection:
```c#
var store = new EventStore(connectionString, cfg => cfg
    .DefineProjection<Cake>(1, builder => builder
        .On<CakeIced>((projection, body, evt) => projection.Color = body.Color)
        .On<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
        .OnOther((projection, evt) => { }) // Any event that hasn't been explicitly listed above
        .OnAny((projection, evt) => { }) // Called on all events, in addition to any of the able
    )
);
```

And then simply read the projection like this:
```c#
var projection = await store.ReadProjection<Cake>(streamId, CancellationToken.None);
```

When you query a projection like this, it will play out all events that have occured since the last query using the
projection definition, rendering the latest projection. The projection is then persisted in the database
so that on the next query it needs only project events that have occured since.