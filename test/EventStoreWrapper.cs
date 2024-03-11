using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public sealed class EventStoreWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    private const String Prefix = "TEST";


    public BlobContainerClient Container { get; }
    public EventStore Sut { get; }

    public EventStoreWrapper(Action<EventStoreConfiguration>? builder = null)
    {
        var containerName = $"unittest-{DateTime.Now:yyyyMMddHHmmss}";
        Container = new(ConnectionString, containerName);
        Sut = new(Container, Prefix, cfg => { builder?.Invoke(cfg); });
    }

    public void AssertSegment(DateOnly date, Int32 sequence, String expected)
    {
        var client = Container.GetAppendBlobClient($"{Prefix}{date:yyyyMMdd}.{sequence}.tsv");
        var actual = client.DownloadContent().Value.Content.ToString();
        actual.Should().Be(expected);
    }

    public void Dispose()
    {
        Container.DeleteIfExists();
        Sut.Dispose();
    }
}