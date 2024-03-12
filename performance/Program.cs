using System.Diagnostics;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using ServcoX.EventSauce;

const String v3ConnectionString = "UseDevelopmentStorage=true;";
const String v2ConnectionString = "UseDevelopmentStorage=true;";
const String aggregateName = "CAKE";

/* Local emulated:
    V3 writes/sec: 94
    V2 writes/sec: 319
    V3 reads/sec: 4525788
    V2 reads/sec: 4076.8
    V3 write+reads/sec: 30.8
    V2 write+reads/sec: 104
*/

/* Standard storage:
    V3 writes/sec: 9.8
    V2 writes/sec: 4
    V3 reads/sec: 4391639.6
    V2 reads/sec: 27.6
    V3 write+reads/sec: 2.6
    V2 write+reads/sec: 1.4
*/

var allowedTime = TimeSpan.FromSeconds(5);

var writeAggregateId = Guid.NewGuid().ToString("N");
var readAggregateId = Guid.NewGuid().ToString("N");
var createdBy = Guid.NewGuid().ToString("N");

var containerName = $"test{Guid.NewGuid():N}";
var container = new BlobContainerClient(v3ConnectionString, containerName);
await container.CreateIfNotExistsAsync();
var v3Store = new ServcoX.EventSauce.V3.EventStore(container, aggregateName, cfg => cfg
    .DoNotSyncBeforeReads());
var v3Projection = v3Store.Project<Cake>(version: 1, cfg => cfg
    .OnCreation((projection, id) => projection.Id = id)
    .OnEvent<CakeBaked>((_, _, _) => { })
    .OnEvent<CakeIced>((projection, body, _) => projection.Color = body.Color)
    .OnEvent<CakeCut>((projection, body, _) => projection.Slices += body.Slices)
    .OnUnexpectedEvent((_, evt) => Console.Error.WriteLine($"Unexpected event ${evt.Type} encountered"))
    .OnAnyEvent((projection, evt) => projection.LastUpdatedAt = evt.At)
    .IndexField(nameof(Cake.Color))
);
var v4Store = new ServcoX.EventSauce.EventStore(container, aggregateName);


var postfix = Guid.NewGuid().ToString("N");
var streamTableName = $"stream{postfix}";
var eventTableName = $"event{postfix}";
var projectionTableName = $"projection{postfix}";

var streamTable = new TableClient(v2ConnectionString, streamTableName);
streamTable.Create();

var eventTable = new TableClient(v2ConnectionString, eventTableName);
eventTable.Create();

var projectionTable = new TableClient(v2ConnectionString, projectionTableName);
projectionTable.Create();

// V3 Writes
var v3Writes = 0;
var v3WriteStopwatch = Stopwatch.StartNew();
do
{
    await v3Store.WriteEvent(writeAggregateId, new CakeBaked());
    v3Writes++;
} while (v3WriteStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 writes/sec: " + (Single)v3Writes / allowedTime.TotalSeconds);

// V3 Reads
await v3Store.WriteEvent(readAggregateId, new CakeBaked(), new Dictionary<String, String> { ["By"] = createdBy });
await v3Store.WriteEvent(readAggregateId, new CakeIced("BLUE"), new Dictionary<String, String> { ["By"] = createdBy });
await v3Store.WriteEvent(readAggregateId, new CakeCut(3), new Dictionary<String, String> { ["By"] = createdBy });
await v3Store.Sync();
var v3Reads = 0;
var v3ReadStopwatch = Stopwatch.StartNew();
do
{
    await v3Projection.Read(readAggregateId);
    v3Reads++;
} while (v3ReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 reads/sec: " + (Single)v3Reads / allowedTime.TotalSeconds);

// V3 Write+Reads
var v3WriteReads = 0;
var v3WriteReadStopwatch = Stopwatch.StartNew();
do
{
    var aggregateId = Guid.NewGuid().ToString("N");
    await v3Store.WriteEvent(aggregateId, new CakeBaked(), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Store.WriteEvent(aggregateId, new CakeIced("BLUE"), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Store.WriteEvent(aggregateId, new CakeCut(3), new Dictionary<String, String> { ["By"] = createdBy });
    await v3Store.Sync();
    await v3Projection.Read(aggregateId);
    v3WriteReads++;
} while (v3WriteReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 write+reads/sec: " + (Single)v3WriteReads / allowedTime.TotalSeconds);

// V4 Writes
var v4Writes = 0;
var v4WriteStopwatch = Stopwatch.StartNew();
do
{
    await v4Store.Write(new CakeBaked());
    v4Writes++;
} while (v4WriteStopwatch.Elapsed < allowedTime);

Console.WriteLine("V4 writes/sec: " + (Single)v4Writes / allowedTime.TotalSeconds);

// V4 Reads
await v4Store.Write(new CakeBaked());
await v4Store.Write(new CakeIced("BLUE"));
await v4Store.Write(new CakeCut(3));
await v4Store.PollNow();
var v4Reads = 0;
var v4ReadStopwatch = Stopwatch.StartNew();
do
{
    await v4Store.Read();
    v4Reads++;
} while (v4ReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V3 reads/sec: " + (Single)v4Reads / allowedTime.TotalSeconds);

// V4 Write+Reads
var v4WriteReads = 0;
var v4WriteReadStopwatch = Stopwatch.StartNew();
do
{
    await v4Store.Write(new CakeBaked());
    await v4Store.Write(new CakeIced("BLUE"));
    await v4Store.Write(new CakeCut(3));
    await v4Store.PollNow();
    await v4Store.Read();
    v4WriteReads++;
} while (v4WriteReadStopwatch.Elapsed < allowedTime);

Console.WriteLine("V4 write+reads/sec: " + (Single)v4WriteReads / allowedTime.TotalSeconds);

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

public readonly record struct CakeBaked : ServcoX.EventSauce.V3.IEventPayload, IEvent;

public readonly record struct CakeIced(String Color) : ServcoX.EventSauce.V3.IEventPayload, IEvent;

public readonly record struct CakeCut(Int32 Slices) : ServcoX.EventSauce.V3.IEventPayload, IEvent;