using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace ServcoX.EventSauce.Tests.Fixtures;

public sealed class EventWrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    public BlobContainerClient Container { get; }
    public EventStore Sut { get; }
    private readonly String _aggregateName;

    public const Int32 MaxBlocksPerSlice = 10;

    public EventWrapper(Action<EventStoreConfiguration>? builder = null)
    {
        var containerName = "unit-tests";
        Container = new(ConnectionString, containerName);
        Container.CreateIfNotExists();

        _aggregateName = Guid.NewGuid().ToString("N").ToUpperInvariant();
        Sut = new(Container, _aggregateName, cfg =>
        {
            cfg.UseTargetBlocksPerSlice(MaxBlocksPerSlice);
            builder?.Invoke(cfg);
        });
    }

    public AppendBlobClient GetSliceClient(Int64 sliceId) =>
        Container.GetAppendBlobClient($"{_aggregateName}/event/{_aggregateName}.{sliceId.ToPaddedString()}.tsv");

    public StreamReader GetSliceStream(Int64 sliceId) =>
        new(GetSliceClient(sliceId).DownloadContent().Value.Content.ToStream());

    public void AppendLine(Int64 sliceId, String block)
    {
        var blob = GetSliceClient(sliceId);
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