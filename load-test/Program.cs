using System.Diagnostics;
using Azure.Storage.Blobs;
using ServcoX.EventSauce;

const String v3ConnectionString = "UseDevelopmentStorage=true;";
const String aggregateName = "CAKE";
var containerName = $"test{Guid.NewGuid():N}";
var container = new BlobContainerClient(v3ConnectionString, containerName);
var store = new EventStore(container, aggregateName);
var allowedTime = TimeSpan.FromSeconds(30);

Console.Write("Seeding...");
var factor = 100;

for (var i = 0; i < factor; i++)
{
    await store.Write(new IEvent[]
    {
        new CakeBaked(),
        new CakeIced("BLUE"),
        new CakeCut(i),
    });
    var progress = Math.Round((Double)i / factor * 100);
    if (progress % 10 == 0) Console.Write($" {progress}%...");
}

Console.WriteLine();

Console.WriteLine($"Testing... ({allowedTime})");
var reads = 0;
var readStopwatch = Stopwatch.StartNew();
do
{
    await store.Read();
    reads++;
} while (readStopwatch.Elapsed < allowedTime);

Console.WriteLine("V4 reads/sec: " + (Single)reads / allowedTime.TotalSeconds);

container.DeleteIfExists();

public readonly record struct CakeBaked : ServcoX.EventSauce.V3.IEventPayload, IEvent;

public readonly record struct CakeIced(String Color) : ServcoX.EventSauce.V3.IEventPayload, IEvent;

public readonly record struct CakeCut(Int32 Slices) : ServcoX.EventSauce.V3.IEventPayload, IEvent;