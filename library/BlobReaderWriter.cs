using System.Globalization;
using System.Text.RegularExpressions;
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

    private const String FileNameDateFormat = "yyyyMMdd";

    // Blob name: {prefix}{date:yyyyMMdd}.{sequence}.tsv

    private static readonly Regex NamePattern = new(@"^(?<prefix>.*)(?<date>\d{8})\.(?<sequence>\d{10})\.tsv$");

    public async Task WriteStream(DateOnly date, Int32 sequence, Stream stream, Int32 targetWritesPerSegment, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (stream.Length == 0) return; // When attempting to write 0 events
        var blob = _containerClient.GetAppendBlobClient(EncodeSegmentName(date, sequence));
        await blob.CreateIfNotExistsAsync(httpHeaders: SliceBlobHeaders, cancellationToken: cancellationToken).ConfigureAwait(false);

        var properties = await blob.GetPropertiesAsync(cancellationToken: cancellationToken).ConfigureAwait(false); // TODO: Don't get properties on _every_ write. Some overage is ok
        if (properties.Value.BlobCommittedBlockCount >= targetWritesPerSegment) throw new TargetWritesExceededException();

        if (stream.Length > blob.AppendBlobMaxAppendBlockBytes)
            throw new TransactionTooLargeException($"Encoded events are {stream.Length} bytes, which exceeds limits of {blob.AppendBlobMaxAppendBlockBytes} bytes. Split events over multiple writes.");

        await blob.AppendBlockAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> ReadStream(DateOnly date, Int32 sequence, Int64 fromOffset, CancellationToken cancellationToken)
    { // TODO: More mature back-off approach
        try
        {
            var blob = _containerClient.GetAppendBlobClient(EncodeSegmentName(date, sequence));
            return await blob.OpenReadAsync(fromOffset, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
        {
        }

        await Task.Delay(100, cancellationToken).ConfigureAwait(false);

        try
        {
            var blob = _containerClient.GetAppendBlobClient(EncodeSegmentName(date, sequence));
            return await blob.OpenReadAsync(fromOffset, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
        {
        }

        await Task.Delay(1000, cancellationToken).ConfigureAwait(false);

        {
            var blob = _containerClient.GetAppendBlobClient(EncodeSegmentName(date, sequence));
            return await blob.OpenReadAsync(fromOffset, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<List<Segment>> ListSegments(CancellationToken cancellationToken)
    {
        var output = new List<Segment>();
        var sliceBlobs = _containerClient.GetBlobsAsync(prefix: _prefix, cancellationToken: cancellationToken);
        await foreach (var sliceBlob in sliceBlobs)
        {
            var segmentName = DecodeSegmentName(sliceBlob.Name);
            if (segmentName is null) continue;
            var end = sliceBlob.Properties.ContentLength ?? 0;

            output.Add(new()
            {
                Date = segmentName.Value.Date,
                Sequence = segmentName.Value.Sequence,
                Length = end,
            });
        }

        return output
            .OrderBy(segment => segment.Date)
            .ThenBy(segment => segment.Sequence)
            .ToList();
    }

    private String EncodeSegmentName(DateOnly date, Int32 sequence) => $"{_prefix}{date.ToString(FileNameDateFormat, CultureInfo.InvariantCulture)}.{sequence.ToPaddedString()}.tsv";

    private (DateOnly Date, Int32 Sequence)? DecodeSegmentName(String name)
    {
        var match = NamePattern.Match(name);
        if (!match.Success) return null;
        var prefix = match.Groups["prefix"].Value;
        if (prefix != _prefix) throw new ArgumentException($"Does not start with required prefix '{_prefix}");
        var date = DateOnly.ParseExact(match.Groups["date"].Value, FileNameDateFormat);
        var sequence = Int32.Parse(match.Groups["sequence"].Value, CultureInfo.InvariantCulture);

        return new(date, sequence);
    }
}