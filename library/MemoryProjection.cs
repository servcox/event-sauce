using System.Collections.Concurrent;
using System.Reflection;
using Stream = ServcoX.EventSauce.Stream;

namespace ServcoX.EventSauce;

[Obsolete("Use EventStore.CreateProjection instead")]
public sealed class MemoryProjector<T> : IDisposable
{
    private readonly String _streamType;
    private readonly EventStore _eventStore;

    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly Thread _refreshThread;
    private Boolean _isDisposed;
    private readonly ConcurrentDictionary<String, Record> _projectionRecords = new();
    private readonly ConcurrentDictionary<String, MethodInfo?> _applyMethodCache = new();
    private readonly Type _underlyingType = typeof(T);
    private readonly Object _syncLock = new();
    private readonly TimeSpan _syncInterval;
    private readonly MethodInfo _fallbackApply;
    private readonly MethodInfo? _promiscuousApply;
    private const String FallbackApplyMethodName = "FallbackApply";
    private const String PromiscuousApplyMethodName = "PromiscuousApply";
    private const String SpecificApplyMethodName = "Apply";

    private sealed class Record
    {
        public String StreamId { get; }
        public T Projection { get; }
        public UInt64 LocalVersion { get; set; }

        public Record(String streamId, T projection)
        {
            StreamId = streamId;
            Projection = projection;
        }
    }

    public MemoryProjector(String streamType, EventStore eventStore, TimeSpan? syncInterval = null)
    {
        _streamType = streamType;
        _eventStore = eventStore;
        _syncInterval = syncInterval ?? TimeSpan.FromSeconds(1);
        _fallbackApply = GetApply(FallbackApplyMethodName, typeof(Event)) ??
                         throw new MissingApplyException($"Projection '{typeof(T).FullName}' is missing apply method. Add `public void {FallbackApplyMethodName}({nameof(Event)})`");
        _promiscuousApply = GetApply(PromiscuousApplyMethodName, typeof(Event));
        _refreshThread = new(RefreshThreadHandler)
        {
            Priority = ThreadPriority.BelowNormal,
            IsBackground = true,
        };
        _refreshThread.Start();
    }

    public ICollection<T> Where(Func<T, Boolean> where) => _projectionRecords.Values.Select(a => a.Projection).Where(where).ToList();
    public T Find(String streamId) => TryFind(streamId) ?? throw new NotFoundException();
    public T? TryFind(String streamId) => _projectionRecords.TryGetValue(streamId, out var record) ? record.Projection : default;

    public void Dispose()
    {
        _isDisposed = true;
        _refreshThread.Join();
    }

    private void RefreshThreadHandler()
    {
        while (!_isDisposed)
        {
            SyncNow();

            Thread.Sleep(_syncInterval);
        }
    }

    public void SyncNow()
    {
        lock (_syncLock)
        {
            var outdatedStreams = GetOutdatedStreams();
            Parallel.ForEach(outdatedStreams, SyncStream);
        }
    }

    private List<Record> GetOutdatedStreams()
    {
        var outstanding = new List<Record>();

        foreach (var stream in _eventStore.ListStreams(_streamType))
        {
            if (!_projectionRecords.TryGetValue(stream.Id, out var record))
            {
                record = _projectionRecords[stream.Id] = new(stream.Id, CreateProjectionInstance(stream));
            }

            if (stream.LatestVersion != record.LocalVersion) outstanding.Add(record);
        }

        return outstanding;
    }

    private void SyncStream(Record record)
    {
        var newEvents = _eventStore.ReadEvents(record.StreamId, record.LocalVersion + 1);
        foreach (var evt in newEvents)
        {
            InvokeApply(record.Projection, evt);
            record.LocalVersion = evt.Version;
        }
    }

    private void InvokeApply(T projection, Event evt)
    {
        _promiscuousApply?.Invoke(projection, new Object[] { evt });
        
        var body = evt.Body;
        if (body is null) // If don't know the event type, handle using fallback Apply
        {
            _fallbackApply.Invoke(projection, new Object[] { evt });
            return;
        }

        var applyMethod = TryGetSpecificApplyForType(body.GetType());
        if (applyMethod is null) // If there's no Apply for this event type, handle using fallback Apply
        {
            _fallbackApply.Invoke(projection, new Object[] { evt });
            return;
        }

        // If we do know the event type and there is an apply, call the specific Apply
        applyMethod.Invoke(projection, new Object[] { body, evt });
    }

    private static T CreateProjectionInstance(Stream stream) => (T)(Activator.CreateInstance(typeof(T), stream.Id) ?? throw new NeverNullException());

    private MethodInfo? TryGetSpecificApplyForType(Type eventTypeName)
    {
        var eventName = eventTypeName.FullName ?? throw new NeverNullException();
        if (!_applyMethodCache.TryGetValue(eventName, out var applyMethod))
        {
            applyMethod = _applyMethodCache[eventName] = GetApply(SpecificApplyMethodName, eventTypeName, typeof(Event));
        }

        return applyMethod;
    }

    private MethodInfo? GetApply(String name, params Type[] arguments) => _underlyingType.GetMethod(name,
        BindingFlags.Instance | BindingFlags.Public,
        Type.DefaultBinder,
        arguments,
        null);
}