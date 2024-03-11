using ServcoX.EventSauce;

// An Aggregate has its internal state, which is a projection of a single fine-grained event stream
const String connectionString = "UseDevelopmentStorage=true;";
const String containerName = "sample-container";

var store = new EventStore(connectionString, containerName, builder => builder
        .CheckForNewEventsEvery(TimeSpan.FromSeconds(10))
        .OnEvent<CakeIced>((evt, metadata) => Console.WriteLine($"Cake iced '{evt.Color}' at {metadata.At}"))
        .OnEvent<CakeCut>((evt, metadata) => Console.WriteLine($"Cake cut into {evt.Slices} slices at {metadata.At}"))
        .OnOtherEvent((evt, metadata) => Console.WriteLine($"Something else (${metadata.Type}) occured at {metadata.At}")) 
);

await store.Write(new CakeBaked());
await store.Write(new CakeIced("BLUE"));
await store.Write(new CakeCut(3));

foreach (var evt in await store.Read())
    Console.WriteLine($"{evt.Type}: {evt.Event}");


await store.PollEvents();

public readonly record struct CakeBaked : IEvent;

public readonly record struct CakeIced(String Color) : IEvent;

public readonly record struct CakeCut(Int32 Slices) : IEvent;