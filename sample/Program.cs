using ServcoX.EventSauce;

const String connectionString = "UseDevelopmentStorage=true;";
const String streamType = "CAKE";
var store = new EventStore(connectionString, cfg => cfg
    .UseStreamTable("stream")
    .UseEventTable("event")
    .UseProjectionTable("projection")
    .DefineProjection<Cake>(streamType: streamType, version: 1, builder => builder
        .OnCreation((projection, id) => projection.Id = id)
        .OnEvent<CakeIced>((projection, body, evt) => projection.Color = body.Color)
        .OnEvent<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
        .OnUnexpectedEvent((projection, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered")) // Called for any event that doesn't have a specific handler
        .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.CreatedAt) // Called for all events - expected and unexpected
        .Index(nameof(Cake.Color), projection => projection.Color)
    )
);
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await store.CreateStream(streamId, streamType);
await store.WriteEvents(streamId, new BakedCake(), userId);
await store.WriteEvents(streamId, new CakeIced("BLUE"), userId);
await store.WriteEvents(streamId, new CakeCut(3), userId);

foreach (var stream in store.ListStreams(streamType))
{
    Console.WriteLine(stream.Id);
}

foreach (var evt in store.ReadEvents(streamId, 0)) // <== Can pick a greater version to only read new events
{
    Console.WriteLine(evt.Version + ": " + evt.Body);
}

var projection = await store.ReadProjection<Cake>(streamId);

var projections = store.ListProjections<Cake>(nameof(Cake.Color), "BLUE");


public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}

public readonly record struct BakedCake : IEventBody;

public readonly record struct CakeIced(String Color) : IEventBody;

public readonly record struct CakeCut(Int32 Slices) : IEventBody;