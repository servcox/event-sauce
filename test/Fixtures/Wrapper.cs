using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Stream = System.IO.Stream;

namespace ServcoX.EventSauce.Tests.Fixtures;

public sealed class Wrapper : IDisposable
{
    private const String ConnectionString = "UseDevelopmentStorage=true;";
    public  BlobContainerClient Container { get; }
    public EventStore Sut { get; }

    public Wrapper()
    {
        var containerName = $"test-{Guid.NewGuid():N}";
        Container = new(ConnectionString, containerName);
        Container.Create();

        const String topic = "TEST";
        Sut = new(topic, Container);
    }

    public Stream GetBlob(String blobName)
    {
        var content = Container.GetBlobClient(blobName).DownloadContent();
        return content.Value.Content.ToStream();
    }

    public void AppendBlock(String blobName, String block)
    {
        var blob = Container.GetAppendBlobClient(blobName);
        blob.CreateIfNotExists();
        blob.AppendBlock(block);
    }

    public void Dispose()
    {
        Container.DeleteIfExists();
    }
}