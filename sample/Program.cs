using ServcoX.EventSauce;

// Write and read events
{
    const String connectionString = "UseDevelopmentStorage=true;";
    const String containerName = "sample-container";

    using var store = new EventStore(connectionString, containerName);
    await store.Write(new CakeBaked());
    await store.Write(new CakeIced("BLUE"));
    await store.Write(new CakeCut(3));
    
    foreach (var evt in await store.Read())
        Console.WriteLine($"{evt.Type}: {evt.Event}");
}

{
    const String connectionString = "UseDevelopmentStorage=true;";
    const String containerName = "sample-container";
    
    using var store = new EventStore(connectionString, containerName, builder => builder
        .PollEvery(TimeSpan.FromMinutes(1)) // <== Check for events created by other writers automatically
        .OnEvent<CakeIced>((evt, metadata) => Console.WriteLine($"Cake iced '{evt.Color}' at {metadata.At}"))
        .OnEvent<CakeCut>((evt, metadata) => Console.WriteLine($"Cake cut into {evt.Slices} slices at {metadata.At}"))
        .OnOtherEvent((evt, metadata) => Console.WriteLine($"Something else (${metadata.Type}) occured at {metadata.At}"))
    );

    await store.Write(new CakeBaked());
    await store.Write(new CakeIced("BLUE"));
    await store.Write(new CakeCut(3));
    
    await store.PollNow(); // <== Check for events created by other writers manually
}


public readonly record struct CakeBaked : IEvent;

public readonly record struct CakeIced(String Color) : IEvent;

public readonly record struct CakeCut(Int32 Slices) : IEvent;