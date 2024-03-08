using System.Collections.Concurrent;
using ServcoX.EventSauce.V3.Exceptions;

namespace ServcoX.EventSauce;

public sealed class EventType
{
    public String Name { get; }

    public EventType (Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        Name = Encode(type);
        KnownEventBodies[Name] = type;
    }
    
    public EventType(String name)
    {
        if (String.IsNullOrEmpty(name)) throw new ArgumentNullOrDefaultException(nameof(name));
        Name = name;
    }
    
    public Type? TryDecode() => KnownEventBodies.GetValueOrDefault(Name);
    
    private static readonly ConcurrentDictionary<String, Type> KnownEventBodies = new();

    public static void Register(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        var eventType = Encode(type);
        KnownEventBodies[eventType] = type;
    }
    
    private static String Encode(Type type)
    {
        if (String.IsNullOrEmpty(type.FullName)) throw new ArgumentException("Unsupported type", nameof(type));
        return type.FullName.ToUpperInvariant();
    }
}