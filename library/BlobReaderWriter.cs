using System.Globalization;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace ServcoX.EventSauce;

public sealed class BlobReaderWriter
{
    private static readonly BlobHttpHeaders SliceBlobHeaders = new()
    {
        ContentType = "text/tab-separated-values",
    };

    private readonly BlobContainerClient _containerClient;
    private readonly String _prefix;

    public BlobReaderWriter(BlobContainerClient containerClient, String prefix)
    {
        _containerClient = containerClient;
        _containerClient.CreateIfNotExists();
        _prefix = prefix;
    }

    private const String PostFix = ".tsv";
    
    private const String FileNameDateFormat = "yyyyMMdd";
    
    private const Int32 FirstRetryDelay = 500;
    private const Int32 SecondRetryDelay = 1000;

    public async Task Write(DateOnly date, Stream stream, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var blob = _containerClient.GetAppendBlobClient(DateToName(date));
        await blob.CreateIfNotExistsAsync(httpHeaders: SliceBlobHeaders, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (stream.Length == 0) return;
        if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
            throw new TransactionTooLargeException($"Encoded events are {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes. Split events over multiple writes.");

        try
        {
            await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (RequestFailedException )
        {
        }

        await Task.Delay(FirstRetryDelay, cancellationToken).ConfigureAwait(false);
        
        try
        {
            await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (RequestFailedException)
        {
        }

        await Task.Delay(SecondRetryDelay, cancellationToken).ConfigureAwait(false);
        
        await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> Read(DateOnly date, Int64 start, CancellationToken cancellationToken)
    {
        var blob = _containerClient.GetAppendBlobClient(DateToName(date));
        return await blob.OpenReadAsync(start, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<Segment>> List(CancellationToken cancellationToken)
    {
        var output = new List<Segment>();
        var sliceBlobs = _containerClient.GetBlobsAsync(prefix: _prefix, cancellationToken: cancellationToken);
        await foreach (var sliceBlob in sliceBlobs)
        {
            var date = NameToDate(sliceBlob.Name);

            var end = sliceBlob.Properties.ContentLength ?? 0;

            output.Add(new()
            {
                Date = date,
                Length = end,
            });
        }

        return output;
    }

    private String DateToName(DateOnly date) => $"{_prefix}{date.ToString(FileNameDateFormat, CultureInfo.InvariantCulture)}{PostFix}";

    private DateOnly NameToDate(String name)
    {
        var raw = name.Substring(1 - FileNameDateFormat.Length - PostFix.Length, FileNameDateFormat.Length);
        var date = DateOnly.ParseExact(raw, FileNameDateFormat);
        return date;
    }
}