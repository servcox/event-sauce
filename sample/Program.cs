using ServcoX.EventSauce;
using ServcoX.EventSauce.Models;

const String connectionString = "UseDevelopmentStorage=true;";
const String streamType = "CAKE";
var store = new EventStore(connectionString, cfg => cfg
    .UseStreamTable("stream")
    .UseEventTable("event")
    .UseProjectionTable("projection")
    .DefineProjection<Cake>(1, builder => builder
        .On<CakeIced>((projection, body, evt) => projection.Color = body.Color)
        .On<CakeCut>((projection, body, evt) => projection.Slices += body.Slices)
        .OnOther((projection, evt) => { })
        .OnAny((projection, evt) => { })
        //.Index(nameof(Cake.Color), projection => projection.Color)
    )
);
var streamId = Guid.NewGuid().ToString();
var userId = Guid.NewGuid().ToString();
await store.CreateStream(streamId, streamType);
await store.WriteStream(streamId, new BakedCake(), userId);
await store.WriteStream(streamId, new CakeIced("BLUE"), userId);
await store.WriteStream(streamId, new CakeCut(3), userId);

foreach (var stream in store.ListStreams(streamType))
{
    Console.WriteLine(stream.Id);
}

foreach (var evt in store.ReadStream(streamId, 0)) // <== Can pick a greater version to only read new events
{
    Console.WriteLine(evt.Version + ": " + evt.Body);
}

var projection = await store.ReadProjection<Cake>(streamId);

// var projections = await store.ListProjections<Cake>(nameof(Cake.Color), "BLUE");


public record Cake
{
    public Int32 Slices { get; set; }
    public String Color { get; set; }
}

public readonly record struct BakedCake : IEventBody;

public readonly record struct CakeIced(String Color) : IEventBody;

public readonly record struct CakeCut(Int32 Slices) : IEventBody;