using Azure.Storage.Blobs;
using Timer = System.Timers.Timer;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global
// ReSharper disable UseAwaitUsing
// ReSharper disable InvertIf

namespace ServcoX.EventSauce;

public sealed class EventStore : IDisposable
{
    private readonly EventStoreConfiguration _configuration = new();
    private readonly BlobReaderWriter _blobReaderWriter;
    private readonly Timer? _syncTimer;
    private Boolean _isDisposed;
    private List<Segment> _segmentsLast = [];
    private readonly SemaphoreSlim _syncLock = new(1);

    public EventStore(String connectionString, String containerName, Action<EventStoreConfiguration>? builder = null) :
        this(new BlobContainerClient(connectionString, containerName), "", builder)
    {
    }

    public EventStore(String connectionString, String containerName, String pathPrefix = "", Action<EventStoreConfiguration>? builder = null) : 
        this(new BlobContainerClient(connectionString, containerName), pathPrefix, builder)
    {
    }

    public EventStore(BlobContainerClient containerClient, String pathPrefix, Action<EventStoreConfiguration>? builder = null)
    {
        ArgumentNullException.ThrowIfNull(containerClient, nameof(containerClient));
        ArgumentNullException.ThrowIfNull(pathPrefix, nameof(pathPrefix));

        builder?.Invoke(_configuration);

        _blobReaderWriter = new(containerClient, pathPrefix);

        if (_configuration.AutoPollInterval > TimeSpan.Zero)
        {
            _syncTimer = new(_configuration.AutoPollInterval);
            _syncTimer.Elapsed += async (_, _) =>
            {
                await PollEvents().ConfigureAwait(false);
                _syncTimer.Start();
            };
            _syncTimer.AutoReset = false;
            _syncTimer.Start();
        }
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

    public async Task<List<Record>> ReadAll(CancellationToken cancellationToken = default)
    {
        var slices = await _blobReaderWriter.List(cancellationToken).ConfigureAwait(false);
        var output = new List<Record>();
        foreach (var slice in slices)
        {
            var events = await ReadDay(slice.Date, 0, cancellationToken).ConfigureAwait(false);
            output.AddRange(events);
        }

        return output;
    }

    public async Task<List<Record>> ReadSince(DateOnly fromDate, CancellationToken cancellationToken = default)
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var output = new List<Record>();
        var date = fromDate;
        while (date <= today)
        {
            var events = await ReadDay(date, 0, cancellationToken).ConfigureAwait(false);
            output.AddRange(events);
            date = date.AddDays(1);
        }

        return output;
    }

    public async Task PollEvents(CancellationToken cancellationToken = default)
    {
        await _syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var segmentsCurrent = await _blobReaderWriter.List(cancellationToken).ConfigureAwait(false);
            foreach (var (date, end) in segmentsCurrent)
            {
                var localEnd = _segmentsLast.FirstOrDefault(segment => segment.Date == date).Length;
                if (localEnd >= end) continue;

                var records = await ReadDay(date, localEnd, cancellationToken).ConfigureAwait(false);
                EventDispatcher.Dispatch(records, _configuration.SpecificEventHandlers, _configuration.OtherEventHandler, _configuration.AnyEventHandler);
            }

            _segmentsLast = segmentsCurrent;
        }
        finally
        {
            _syncLock.Release();
        }
    }

    private async Task<List<Record>> ReadDay(DateOnly date, Int64 start = 0, CancellationToken cancellationToken = default)
    {
        using var stream = await _blobReaderWriter.Read(date, start, cancellationToken).ConfigureAwait(false);
        var events = EventStream.Decode(stream);
        return events;
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _syncTimer?.Dispose();
        _syncLock.Dispose();
    }
}