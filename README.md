# ServcoX.EventSauce
Event Sauce is the light-weight event sourcing library Servco uses internally to store our events in Azure Table Storage. 
Its for when you want to use event sourcing but your needs (or budget) aren't demanding enough to justify using Kafka. 

It's performant a modest scale, and since it backs on Azure Table Storage it's cost is tiny compared to just about 
everything else. It's also simple, and allows you to build out event sourcing in a way that suits you.

# Installation
Grab it from NuGet from `dotnet add package ServcoX.EventSauce` or `dotnet add package ServcoX.EventSauce.DependencyInjection` for DI support.

# Basic usage
Define your events like this:
```c#
public readonly record struct CakeBaked;
public readonly record struct CakeIced(String Color);
public readonly record struct CakeCut(Int32 Slices);
```

Connect to your event store like this:
```c#
var eventStore = new EventStore(connectionString: "UseDevelopmentStorage=true;", containerName: "EventSauce", aggregateName: "CAKE");
```

Or if you're using Microsoft DI, then you can use this:
```c#
builder.Services.AddEventSauce(connectionString: "UseDevelopmentStorage=true;", containerName: "EventSauce", aggregateName: "CAKE");
```

Write events like this:
```c#
var aggregateId = Guid.NewGuid().ToString("N");
await store.WriteEvent(aggregateId, new CakeBaked());
await store.WriteEvent(aggregateId, new CakeIced("BLUE"));
await store.WriteEvent(aggregateId, new CakeCut(3));
```

And finally, read events back like here:
```c#
foreach (var slice in await store.ListSlices())
{
    foreach (var evt in await store.ReadEvents(slice.Id))
    {
        Console.WriteLine($"{evt.Type}: {evt.Payload}");
    }
}
```

# Projections
Once you have events being stored, you can then go a step further and create projections based on them.

Create a projection like this:
```c#
public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}
```

When you're creating your store, define how to build the projection:
```c#
var cakeProjection = store.Project<Cake>(version: 1, builder => builder
    .OnCreation((projection, id) => projection.Id = id)
    .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
    .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
    .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
    .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.At) // Called for all events - expected and unexpected
    .IndexField(nameof(Cake.Color))
);
```

Then simply read the projection like this:
```c#
var aggregate = await cakeProjection.Read(aggregateId);
```

Or query on a field that has been indexed (see `.Index` above):
```c#
var aggregates = cakeProjection.Query(nameof(Cake.Color), "BLUE");
```

When you query a projection it will play out all events that have occured since the last query using the
projection definition, rendering the latest projection. The projection is then persisted in the database
so that on the next query it needs only project events that have occured since.