using Azure.Storage.Blobs;
using ServcoX.EventSauce;

const String connectionString = "UseDevelopmentStorage=true;";
const String containerName = "test";
const String topic = "CAKE";

var container = new BlobContainerClient(connectionString, containerName);
await container.CreateIfNotExistsAsync();
var eventStore = new EventStore(topic, container);

await eventStore.Write(new BakedCake());
await eventStore.Write(new CakeIced("BLUE"));
await eventStore.Write(new CakeCut(3));

foreach (var evt in await eventStore.Read())
{
    Console.WriteLine($"{evt.Type}: {evt.Payload}");
}



// var projectionStore = new ProjectionStore(cfg => cfg
//     .UseStreamTable("stream")
//     .UseEventTable("event")
//     .UseProjectionTable("projection")
//     .RefreshProjectionsAfterWriting()
//     .DefineProjection<Cake>(streamType: streamType, version: 1, builder => builder
//         .OnCreation((projection, id) => projection.Id = id)
//         .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
//         .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
//         .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
//         .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.CreatedAt) // Called for all events - expected and unexpected
//         .Index(nameof(Cake.Color), projection => projection.Color)
//     ))
// var projection = await eventStore.ReadProjection<Cake>(streamId);
//
// var projections = eventStore.ListProjections<Cake>(nameof(Cake.Color), "BLUE");


public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}

public readonly record struct BakedCake;

public readonly record struct CakeIced(String Color);

public readonly record struct CakeCut(Int32 Slices);