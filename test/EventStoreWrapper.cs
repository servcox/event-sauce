using System.Diagnostics;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using FluentAssertions;

namespace ServcoX.EventSauce.Tests;

public sealed class EventStoreWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    private const String Prefix = "TEST";
    public const Int32 TargetWritesPerSegment = 10;

    public BlobContainerClient Container { get; }
    public EventStore Sut { get; }

    public EventStoreWrapper(Action<EventStoreConfiguration>? builder = null)
    {
        var containerName = $"unittest-{DateTime.Now:yyyyMMddHHmmss}";
        Container = new(ConnectionString, containerName);
        Sut = new(Container, Prefix, cfg =>
        {
            cfg.UseTargetWritesPerSegment(TargetWritesPerSegment);
            builder?.Invoke(cfg);
        });
    }

    public void AssertSegment(DateOnly date, Int32 sequence, String expected)
    {
        var blob = GetSegmentBlobClient(date, sequence);
        var actual = blob.DownloadContent().Value.Content.ToString();
        actual.Should().Be(expected);
    }

    private AppendBlobClient GetSegmentBlobClient(DateOnly date, Int32 sequence)
    {
        var client = Container.GetAppendBlobClient($"{Prefix}{date:yyyyMMdd}.{sequence}.tsv");
        client.CreateIfNotExists();
        return client;
    }

    public void Dispose()
    {
        Container.DeleteIfExists();
        Sut.Dispose();
    }
}