using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace ServcoX.EventSauce;

public sealed class BlobReaderWriter(String aggregateName, BlobContainerClient containerClient)
{
    private static readonly BlobHttpHeaders SliceBlobHeaders = new()
    {
        ContentType = "text/tab-separated-values",
    };

    private const String FileExtension = "tsv";

    public async Task Write(DateOnly date, Stream stream, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var blob = containerClient.GetAppendBlobClient(DateToName(date));
        await blob.CreateIfNotExistsAsync(httpHeaders: SliceBlobHeaders, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (stream.Length == 0) return;
        if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
            throw new TransactionTooLargeException($"Encoded events are {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes. Split events over multiple writes.");
        await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> Read(DateOnly date, Int64 start, CancellationToken cancellationToken)
    {
        var blob = containerClient.GetAppendBlobClient(DateToName(date));
        return await blob.OpenReadAsync(start, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<Segment>> List(CancellationToken cancellationToken)
    {
        var output = new List<Segment>();
        var sliceBlobs = containerClient.GetBlobsAsync(prefix: $"{aggregateName}.", cancellationToken: cancellationToken);
        await foreach (var sliceBlob in sliceBlobs)
        {
            var date = NameToDate(sliceBlob.Name);

            var end = sliceBlob.Properties.ContentLength ?? 0;

            output.Add(new()
            {
                Date = date,
                End = end,
            });
        }

        return output;
    }

    private String DateToName(DateOnly date) => $"{aggregateName}.{date:yyyyMMdd}.{FileExtension}";

    private DateOnly NameToDate(String name)
    {
        var tokens = name.Split('.');
        if (tokens.Length != 3) throw new ArgumentException("Must contain exactly two periods", nameof(name));
        if (tokens[0] != aggregateName) throw new ArgumentException($"Must start with aggregateName ('{aggregateName}')", nameof(name));
        if (tokens[2] != FileExtension) throw new ArgumentException($"Must emd with extension ('{FileExtension}')", nameof(name));
        
        var date = DateOnly.ParseExact(tokens[1], "yyyyMMdd");
        return date;
    }
}