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
var eventStore = new EventStore("=== connection string goes here ===");
```

Create a stream and write events like this:
```c#
var streamType = "CAKE";
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await eventStore.CreateStream(streamId, streamType);
await eventStore.WriteEvents(streamId, new BakedCake(), userId);
await eventStore.WriteEvents(streamId, new IcedCake("BLUE"), userId);
await eventStore.WriteEvents(streamId, new CutCake(3), userId);
```

Get a list of streams you've already created like so:
```c#
foreach (var stream in eventStore.ListStreams(streamType)) Console.WriteLine(stream.Id);
```

And finally, read events back like here:
```c#
var minVersion = 0; // <== Can pick a greater version to only read new events
foreach (var evt in eventStore.ReadEvents(streamId, minVersion)) Console.WriteLine(evt.Version + ": " + evt.Body);
```

# Projections
Once you have events being stored, you can then go a step further and create projections based on them.

Create a projection like this:
```c#
public record Cake
{
    public String Id { get; set; }
    public Int32 Slices { get; set; }
    public String Color { get; set; }
    public DateTime LastUpdatedAt { get; set; }
}
```

When you're creating your store, define how to build the projection:
```c#
var store = new EventStore(connectionString, cfg => cfg
    .RefreshProjectionsAfterWriting()
    .DefineProjection<Cake>(streamType: "CAKE", version: 1, builder => builder
        .OnCreation((projection, id) => projection.Id = id)
        .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
        .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
        .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
        .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.CreatedAt) // Called for all events - expected and unexpected
        .Index(nameof(Cake.Color), projection => projection.Color)
    )
);
```

Then simply read the projection like this:
```c#
var projection = await store.ReadProjection<Cake>(streamId);
```

Or query on a field that has been indexed (see `.Index` above):
```c#
var projections = store.ListProjections<Cake>(nameof(Cake.Color), "BLUE");
```

When you query a projection it will play out all events that have occured since the last query using the
projection definition, rendering the latest projection. The projection is then persisted in the database
so that on the next query it needs only project events that have occured since.