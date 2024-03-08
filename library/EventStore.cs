using Azure.Storage.Blobs;

namespace ServcoX.EventSauce;

public sealed class EventStore
{
    private readonly BlobReaderWriter _blobReaderWriter;

    public EventStore(AggregateName aggregateName, String connectionString, String containerName) : this(aggregateName,
        new(connectionString, containerName))
    {
    }

    public EventStore(AggregateName aggregateName, BlobContainerClient containerClient)
    {
        ArgumentNullException.ThrowIfNull(aggregateName, nameof(aggregateName));
        ArgumentNullException.ThrowIfNull(containerClient, nameof(containerClient));

        _blobReaderWriter = new(aggregateName, containerClient);
    }

    public Task Write(IEvent evt, CancellationToken cancellationToken = default) =>
        Write(evt, DateTime.UtcNow, cancellationToken);

    public Task Write(IEvent evt, DateTime at, CancellationToken cancellationToken = default) =>
        Write([evt], at, cancellationToken);

    public Task Write(IEnumerable<IEvent> events, CancellationToken cancellationToken = default) =>
        Write(events, DateTime.UtcNow, cancellationToken);

    public async Task Write(IEnumerable<IEvent> events, DateTime at, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);

        using var stream = EventStream.Encode(events, at);
        var date = DateOnly.FromDateTime(at);
        await _blobReaderWriter.Write(date, stream, cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<EventRecord>> ReadAllFromDate(DateOnly fromDate, CancellationToken cancellationToken = default)
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var output = new List<EventRecord>();
        var date = fromDate;
        while (date <= today)
        {
            var events = await ReadForDate(date, 0, cancellationToken).ConfigureAwait(false);
            output.AddRange(events);
            date = date.AddDays(1);
        }

        return output;
    }

    public async Task<List<EventRecord>> ReadForDate(DateOnly date, Int64 start = 0, CancellationToken cancellationToken = default)
    {
        using var stream = await _blobReaderWriter.Read(date, start, cancellationToken).ConfigureAwait(false);
        var events = EventStream.Decode(stream);
        return events;
    }

    public async Task<List<Segment>> Summarize(CancellationToken cancellationToken) =>
        await _blobReaderWriter.List(cancellationToken).ConfigureAwait(false);
}