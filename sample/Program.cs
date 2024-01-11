using Azure.Storage.Blobs;
using ServcoX.EventSauce;

// An Aggregate has its internal state, which is a projection of a single fine-grained event stream
const String connectionString = "UseDevelopmentStorage=true;";
const String containerName = "test";
const String aggregateName = "CAKE";

var container = new BlobContainerClient(connectionString, containerName);
await container.CreateIfNotExistsAsync();
var store = new EventStore(container, aggregateName, cfg => cfg
    .SyncEvery(TimeSpan.FromMinutes(15))
    .DoNotSyncBeforeReads());

var aggregateId = Guid.NewGuid().ToString("N");
await store.WriteEvent(aggregateId, new CakeBaked());
await store.WriteEvent(aggregateId, new CakeIced("BLUE"));
await store.WriteEvent(aggregateId, new CakeCut(3));

foreach (var slice in await store.ListSlices())
{
    foreach (var evt in await store.ReadEvents(slice.Id))
    {
        Console.WriteLine($"{evt.Type}: {evt.Payload}");
    }
}

var cakeProjection = store.Project<Cake>(version: 1, builder => builder
    .OnCreation((projection, id) => projection.Id = id)
    .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
    .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
    .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
    .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.At) // Called for all events - expected and unexpected
    .IndexField(nameof(Cake.Color))
);

await store.Sync();
var aggregate = await cakeProjection.Read(aggregateId);

var aggregates = cakeProjection.Query(nameof(Cake.Color), "BLUE");


public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}

public readonly record struct CakeBaked : IEventPayload;

public readonly record struct CakeIced(String Color) : IEventPayload;

public readonly record struct CakeCut(Int32 Slices) : IEventPayload;