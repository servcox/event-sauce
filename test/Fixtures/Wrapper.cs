using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using ServcoX.EventSauce.ConfigurationBuilders;
using ServcoX.EventSauce.Extensions;

namespace ServcoX.EventSauce.Tests.Fixtures;

public sealed class Wrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    public BlobContainerClient Container { get; }
    public EventStore Sut { get; }
    private readonly String _topic;

    public const Int32 MaxBlocksPerSlice = 10;

    public Wrapper(Action<EventStoreConfiguration>? builder = null)
    {
        var containerName = "unit-tests";
        Container = new(ConnectionString, containerName);
        Container.CreateIfNotExists();

        _topic = Guid.NewGuid().ToString("N").ToUpperInvariant();
        Sut = new(_topic, Container, cfg =>
        {
            cfg.UseTargetBlocksPerSlice(MaxBlocksPerSlice);
            builder?.Invoke(cfg);
        });
    }

    public AppendBlobClient GetSliceClient(UInt64 slice) =>
        Container.GetAppendBlobClient($"{_topic}.{slice.ToPaddedString()}.tsv");

    public StreamReader GetSliceStream(UInt64 slice) =>
        new(GetSliceClient(slice).DownloadContent().Value.Content.ToStream());

    public void AppendLine(UInt64 slice, String block)
    {
        var blob = GetSliceClient(slice);
        blob.CreateIfNotExists();
        blob.AppendBlock(block + "\n");
    }

    public void PrepareForOverlappingWrite()
    {
        var blob = GetSliceClient(0);
        blob.CreateIfNotExists();

        using var stream = new MemoryStream("\n"u8.ToArray());
        for (var i = 0; i < MaxBlocksPerSlice - 1; i++)
        {
            blob.AppendBlock(stream);
            stream.Rewind();
        }
    }

    public void Dispose()
    {
        GetSliceClient(0).DeleteIfExists();
        GetSliceClient(1).DeleteIfExists();
    }
}