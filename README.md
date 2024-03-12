# ServcoX.EventSauce
So you want to do event sourcing in your project? Great, does your project meet the following requirements?
* Low load (less than <50 writes/sec)
* .NET 7 or newer
* Using Azure cloud

Yes? Then Event Sauce is for you. It's a simple event sourcing library that uses Azure Blob Storage as it's backend. 
It's super cheap to run and doesn't require any storage servers or messaging infrastructure. It's the tool to use
when Kafka is overkill for your project.

# Installation
Grab it from NuGet using `dotnet add package ServcoX.EventSauce`.

# Basic usage
Define your events like this:
```c#
public readonly record struct CakeBaked : IEvent;
public readonly record struct CakeIced(String Color) : IEvent;
public readonly record struct CakeCut(Int32 Slices) : IEvent;
```

Connect to your event store like this:
```c#
const String connectionString = "UseDevelopmentStorage=true;";
const String containerName = "sample-container";
using var store = new EventStore(connectionString, containerName);
```

Write events like this:
```c#
await store.Write(new CakeBaked());
await store.Write(new CakeIced("BLUE"));
await store.Write(new CakeCut(3));
```

Read back events like this:
```c#
foreach (var evt in await store.Read())
    Console.WriteLine($"{evt.Type}: {evt.Event}");
```

If you want to only read events from a certain date use something like:
```c#
var events = store.Read(new DateOnly(2000, 1, 1))
```

# Projections
Once you have events being stored, you can then go a step further and create projections based on them.

Create a projection like this:
```c#
using var store = new EventStore(connectionString, containerName, builder => builder
    .PollEvery(TimeSpan.FromMinutes(1)) // <== Check for events created by other writers automatically
    .OnEvent<CakeIced>((evt, metadata) => Console.WriteLine($"Cake iced '{evt.Color}' at {metadata.At}"))
    .OnEvent<CakeCut>((evt, metadata) => Console.WriteLine($"Cake cut into {evt.Slices} slices at {metadata.At}"))
    .OnOtherEvent((evt, metadata) => Console.WriteLine($"Something else (${metadata.Type}) occured at {metadata.At}"))
);
```

Automatically your code is called whenever an event is raised in your application, or any others writing to the same 
target. You can use those events to populate your projection in a database like [LiteDB](https://www.litedb.org/) or 
[SQLite](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/?tabs=netcore-cli).

Note that you can change the poll interval to control your consistency.

You can also trigger a manual poll to immediately trigger the callbacks for any events that hadn't yet been recieved:

```c#
await store.PollNow();
```

# Version 4
This is a significant rewrite from version 3. Version 3's client has been moved to the `ServcoX.EventSauce.V3` namespace 
and is useful for migrations. Version 4's persisted data is incompatible with version 3 and a migration is required