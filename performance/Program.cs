using System.Diagnostics;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using ServcoX.EventSauce.V2;
using EventStore = ServcoX.EventSauce.EventStore;

const String connectionString = "UseDevelopmentStorage=true;";

const String aggregateName = "CAKE";


var allowedTime = TimeSpan.FromSeconds(5);

var writeAggregateId = Guid.NewGuid().ToString("N");
var readAggregateId = Guid.NewGuid().ToString("N");
var createdBy = Guid.NewGuid().ToString("N");

var containerName = $"test{Guid.NewGuid():N}";
var container = new BlobContainerClient(connectionString, containerName);
await container.CreateIfNotExistsAsync();
var v3Store = new EventStore(container, aggregateName);
var v3Projection = v3Store.Project<Cake>(version: 1, builder => builder
    .OnCreation((projection, id) => projection.Id = id)
    .OnEvent<CakeBaked>((projection, body, _) => { })
    .OnEvent<CakeIced>((projection, body, _) => projection.Color = body.Color)
    .OnEvent<CakeCut>((projection, body, _) => projection.Slices += body.Slices)
    .OnUnexpectedEvent((_, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered"))
    .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.At)
    .IndexField(nameof(Cake.Color))
);


var postfix = Guid.NewGuid().ToString("N");
var streamTableName = $"stream{postfix}";
var eventTableName = $"event{postfix}";
var projectionTableName = $"projection{postfix}";

var streamTable = new TableClient(connectionString, streamTableName);
streamTable.Create();

var eventTable = new TableClient(connectionString, eventTableName);
eventTable.Create();

var projectionTable = new TableClient(connectionString, projectionTableName);
projectionTable.Create();

var v2Store = new ServcoX.EventSauce.V2.EventStore(connectionString, cfg => cfg
    .UseStreamTable(streamTableName)
    .UseEventTable(eventTableName)
    .UseProjectionTable(projectionTableName)
    .RefreshProjectionsAfterWriting()
    .DefineProjection<Cake>(streamType: "CAKE", version: 1, builder => builder
        .OnCreation((projection, id) => projection.Id = id)
        .OnEvent<CakeBaked>((projection, body, _) => { })
        .OnEvent<CakeIced>((projection, body, _) => projection.Color = body.Color)
        .OnEvent<CakeCut>((projection, body, _) => projection.Slices += body.Slices)
        .OnUnexpectedEvent((_, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered"))
        .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.CreatedAt)
        .Index(nameof(Cake.Color), projection => projection.Color)
    ));

// V3 Writes
var v3Writes = 0;
var v3WriteStopwatch = Stopwatch.StartNew();
do
{
    await v3Store.WriteEvent(writeAggregateId, new CakeBaked());
    v3Writes++;
} while (v3WriteStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 writes/sec: " + (Single)v3Writes / allowedTime.TotalSeconds);

// V2 Writes
var v2Writes = 0;
var v2WriteStopwatch = Stopwatch.StartNew();
await v2Store.CreateStream(writeAggregateId, aggregateName);
do
{
    await v2Store.WriteEvents(writeAggregateId, new CakeBaked(), createdBy);
    v2Writes++;
} while (v2WriteStopwatch.Elapsed < allowedTime);

Console.WriteLine("V2 writes/sec: " + (Single)v2Writes / allowedTime.TotalSeconds);

// V3 Reads
await v3Store.WriteEvent(readAggregateId, new CakeBaked(), new Dictionary<String, String> { ["By"] = createdBy });
await v3Store.WriteEvent(readAggregateId, new CakeIced("BLUE"), new Dictionary<String, String> { ["By"] = createdBy });
await v3Store.WriteEvent(readAggregateId, new CakeCut(3), new Dictionary<String, String> { ["By"] = createdBy });
var v3Reads = 0;
var v3ReadStopwatch = Stopwatch.StartNew();
do
{
    await v3Projection.Read(readAggregateId);
    v3Reads++;
} while (v3ReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 reads/sec: " + (Single)v3Reads / allowedTime.TotalSeconds);

// V2 Reads
await v2Store.CreateStream(readAggregateId, aggregateName);
await v2Store.WriteEvents(readAggregateId, new CakeBaked(), createdBy);
await v2Store.WriteEvents(readAggregateId, new CakeIced("BLUE"), createdBy);
await v2Store.WriteEvents(readAggregateId, new CakeCut(3), createdBy);
var v2Reads = 0;
var v2ReadStopwatch = Stopwatch.StartNew();
do
{
    await v2Store.ReadProjection<Cake>(readAggregateId);
    v2Reads++;
} while (v2ReadStopwatch.Elapsed < allowedTime);

// V3 Write+Reads
var v3WriteReads = 0;
var v3WriteReadStopwatch = Stopwatch.StartNew();
do
{
    var aggregateId = Guid.NewGuid().ToString("N");
    await v3Store.WriteEvent(aggregateId, new CakeBaked(), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Store.WriteEvent(aggregateId, new CakeIced("BLUE"), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Store.WriteEvent(aggregateId, new CakeCut(3), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Projection.Read(aggregateId);
    v3WriteReads++;
} while (v3WriteReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 write+reads/sec: " + (Single)v3WriteReads / allowedTime.TotalSeconds);

// V2 Write+Reads
var v2WriteReads = 0;
var v2WriteReadStopwatch = Stopwatch.StartNew();
do
{
    var aggregateId = Guid.NewGuid().ToString("N");
    await v2Store.CreateStream(aggregateId, aggregateName);
    await v2Store.WriteEvents(aggregateId, new CakeBaked(), createdBy);
    await v2Store.WriteEvents(aggregateId, new CakeIced("BLUE"), createdBy);
    await v2Store.WriteEvents(aggregateId, new CakeCut(3), createdBy);
    await v2Store.ReadProjection<Cake>(aggregateId);
    v2WriteReads++;
} while (v2WriteReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V2 write+reads/sec: " + (Single)v2WriteReads / allowedTime.TotalSeconds);

container.DeleteIfExists();

streamTable.Delete();
eventTable.Delete();
projectionTable.Delete();

public record Cake
{
    public String Id { get; set; } = String.Empty;
    public Int32 Slices { get; set; }
    public String Color { get; set; } = String.Empty;
    public DateTime LastUpdatedAt { get; set; }
}

public readonly record struct CakeBaked : IEventBody;

public readonly record struct CakeIced(String Color) : IEventBody;

public readonly record struct CakeCut(Int32 Slices) : IEventBody;