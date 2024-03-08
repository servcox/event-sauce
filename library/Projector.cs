using Timer = System.Timers.Timer;

namespace ServcoX.EventSauce;

public sealed class Projector : IDisposable
{
    private readonly EventStore _eventStore;

    private readonly ProjectorConfiguration _configuration = new();
    private readonly Timer _syncTimer;
    private Boolean _isDisposed;
    private readonly Dictionary<DateOnly, Int64> _localEnds = new(); // Slice Date => End;
    private readonly SemaphoreSlim _syncLock = new(1);

    public Projector(EventStore eventStore, Action<ProjectorConfiguration> builder)
    {
        ArgumentNullException.ThrowIfNull(eventStore, nameof(eventStore));
        ArgumentNullException.ThrowIfNull(builder, nameof(builder));

        builder.Invoke(_configuration);

        _eventStore = eventStore;

        _syncTimer = new(_configuration.SyncInterval);
        _syncTimer.Elapsed += async (_, _) =>
        {
            await Sync().ConfigureAwait(false);
            _syncTimer.Start();
        };
        _syncTimer.AutoReset = false;
        _syncTimer.Start();
    }

    // TODO: Registering events
    
    public async Task Sync(CancellationToken cancellationToken = default)
    {
        await _syncLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var segments = await _eventStore.Summarize(cancellationToken).ConfigureAwait(false);
            foreach (var (date, end) in segments)
            {
                _localEnds.TryGetValue(date, out var localEnd);
                if (localEnd >= end) continue;
                _localEnds[date] = end;

                var records = await _eventStore.ReadForDate(date, localEnd,  cancellationToken).ConfigureAwait(false);
                
                foreach (var record in records)
                {

                    var type = record.Type.TryDecode();
                    
                    if (type is not null && _configuration.EventHandlers.TryGetValue(type, out var handlers))
                    {
                        handlers.Invoke(record.Event, record);
                    }
                    else
                    {
                        _configuration.UnknownEventHandler.Invoke(record.Event, record);
                    }

                    _configuration.AnyEventHandler.Invoke(record.Event, record);
                }
            }
        }
        finally
        {
            _syncLock.Release();
        }
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _syncTimer.Dispose();
        _syncLock.Dispose();
    }
}