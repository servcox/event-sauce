using Azure.Storage.Blobs;
using ServcoX.EventSauce;

const String connectionString = "UseDevelopmentStorage=true;";
const String containerName = "test";
const String aggregateName = "CAKE";

var container = new BlobContainerClient(connectionString, containerName);
await container.CreateIfNotExistsAsync();
var eventStore = new EventStore(aggregateName, container);

var aggregateId = Guid.NewGuid().ToString("N");
await eventStore.Write(aggregateId, new CakeBaked());
await eventStore.Write(aggregateId, new CakeIced("BLUE"));
await eventStore.Write(aggregateId, new CakeCut(3));

foreach (var evt in await eventStore.Read())
{
    Console.WriteLine($"{evt.Type}: {evt.Payload}");
}

var projectionStore = new ProjectionStore(eventStore, cfg => cfg
    .DefineProjection<Cake>(version: 1, builder => builder
        .OnCreation((projection, id) => projection.Id = id)
        .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
        .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
        .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
        .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.At) // Called for all events - expected and unexpected
        .IndexField(nameof(Cake.Color))
    ));

var projection = await projectionStore.Read<Cake>(aggregateId);

var projections = projectionStore.Query<Cake>(nameof(Cake.Color), "BLUE");


public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}

public readonly record struct CakeBaked;

public readonly record struct CakeIced(String Color);

public readonly record struct CakeCut(Int32 Slices);