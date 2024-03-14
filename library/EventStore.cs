using System.Collections.Concurrent;
using Azure;
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
    private readonly SemaphoreSlim _syncLock = new(1);
    private readonly ConcurrentDictionary<DateOnly, Int32> _currentSegment = new();
    private Boolean _isDisposed;
    private List<Segment> _lastSegments = [];

    public EventStore(String connectionString, String containerName, Action<EventStoreConfiguration>? builder = null) :
        this(new BlobContainerClient(connectionString, containerName), "", builder)
    {
    }

    public EventStore(String connectionString, String containerName, String pathPrefix, Action<EventStoreConfiguration>? builder = null) :
        this(new BlobContainerClient(connectionString, containerName), pathPrefix, builder)
    {
    }

    public EventStore(BlobContainerClient containerClient, String pathPrefix, Action<EventStoreConfiguration>? builder = null)
    {
        ArgumentNullException.ThrowIfNull(containerClient, nameof(containerClient));
        ArgumentNullException.ThrowIfNull(pathPrefix, nameof(pathPrefix));

        builder?.Invoke(_configuration);

        _blobReaderWriter = new(containerClient, pathPrefix);

        PollNow().Wait();

        if (_configuration.AutoPollInterval > TimeSpan.Zero)
        {
            _syncTimer = new(_configuration.AutoPollInterval);
            _syncTimer.Elapsed += async (_, _) =>
            {
                await PollNow().ConfigureAwait(false);
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
        _currentSegment.TryGetValue(date, out var segment);

        while (true)
        {
            try
            {
                await _blobReaderWriter.WriteStream(date, segment, stream, _configuration.TargetWritesPerSegment, cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (TargetWritesExceededException)
            {
                _currentSegment[date] = ++segment;
                stream.Rewind();
            }
        }
    }

    public async Task<List<Record>> Read(DateOnly? fromDate = null, CancellationToken cancellationToken = default)
    {
        var slices = await _blobReaderWriter.ListSegments(cancellationToken).ConfigureAwait(false);
        if (fromDate is not null) slices = slices.Where(slice => slice.Date >= fromDate).ToList();

        var output = new List<Record>();
        foreach (var slice in slices)
        {
            var events = await ReadSegment(slice.Date, slice.Sequence, 0, cancellationToken).ConfigureAwait(false);
            output.AddRange(events);
        }

        return output;
    }

    public async Task PollNow(CancellationToken cancellationToken = default)
    {
        await _syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var segments = await _blobReaderWriter.ListSegments(cancellationToken).ConfigureAwait(false);
            foreach (var (date, sequence, length) in segments)
            {
                var lastLength = _lastSegments.SingleOrDefault(segment => segment.Date == date && segment.Sequence == sequence).Length;
                if (lastLength >= length) continue;

                var records = await ReadSegment(date, sequence, lastLength, cancellationToken).ConfigureAwait(false);
                EventDispatcher.Dispatch(records, _configuration.SpecificEventHandlers, _configuration.OtherEventHandler, _configuration.AnyEventHandler);
            }

            _lastSegments = segments;
        }
        finally
        {
            if (!_isDisposed) _syncLock.Release();
        }
    }

    private async Task<List<Record>> ReadSegment(DateOnly date, Int32 sequence, Int64 offset = 0, CancellationToken cancellationToken = default)
    {
        // TODO: More mature retry
        try
        {
            using var stream = await _blobReaderWriter.ReadStream(date, sequence, offset, cancellationToken).ConfigureAwait(false);
            var events = EventStream.Decode(stream);
            return events;
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
        {
        }

        await Task.Delay(100, cancellationToken).ConfigureAwait(false);

        try
        {
            using var stream = await _blobReaderWriter.ReadStream(date, sequence, offset, cancellationToken).ConfigureAwait(false);
            var events = EventStream.Decode(stream);
            return events;
        }
        catch (RequestFailedException ex) when (ex.ErrorCode == "ConditionNotMet")
        {
        }

        await Task.Delay(1000, cancellationToken).ConfigureAwait(false);

        {
            using var stream = await _blobReaderWriter.ReadStream(date, sequence, offset, cancellationToken).ConfigureAwait(false);
            var events = EventStream.Decode(stream);
            return events;
        }
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _syncTimer?.Dispose();
        _syncLock.Dispose();
    }
}