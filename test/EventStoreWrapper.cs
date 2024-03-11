using Azure.Storage.Blobs;

namespace ServcoX.EventSauce.Tests;

public sealed class EventStoreWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    private const String Prefix = "TEST";


    public BlobContainerClient Container { get; }
    public EventStore Sut { get; }

    public EventStoreWrapper(Action<EventStoreConfiguration>? builder = null)
    {
        var containerName = "unit-test-" + DateTime.Now.ToString("o");
        Container = new(ConnectionString, containerName);
        Sut = new(Container, Prefix, cfg => { builder?.Invoke(cfg); });
    }

    public void Dispose()
    {
        Container.DeleteIfExists();
        Sut.Dispose();
    }
}