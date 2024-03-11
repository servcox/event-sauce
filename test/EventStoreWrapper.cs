using System.Diagnostics;
using System.Text;
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
        
        EventType.Register<TestData.TestEventA>();
        EventType.Register<TestData.TestEventB>();
    }

    public void AssertSegment(DateOnly date, Int32 sequence, String expected)
    {
        var blob = GetSegmentBlobClient(date, sequence);
        var actual = blob.DownloadContent().Value.Content.ToString();
        actual.Should().Be(expected);
    }
    
    public void WriteSegment(DateOnly date, Int32 sequence, String raw)
    {
        var blob = GetSegmentBlobClient(date, sequence);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(raw));
        blob.AppendBlock(stream);
    }

    private AppendBlobClient GetSegmentBlobClient(DateOnly date, Int32 sequence)
    {
        var blob = Container.GetAppendBlobClient($"{Prefix}{date:yyyyMMdd}.{sequence}.tsv");
        blob.CreateIfNotExists();
        return blob;
    }

    public void Dispose()
    {
        Container.DeleteIfExists();
        Sut.Dispose();
    }

  
}