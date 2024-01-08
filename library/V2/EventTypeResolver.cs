using System.Reflection;
using ServcoX.EventSauce.Exceptions;
// ReSharper disable MemberCanBeMadeStatic.Global

namespace ServcoX.EventSauce.V2;

public sealed class EventTypeResolver
{
    private readonly Dictionary<String, Type> _knownEventBodies;

    public EventTypeResolver()
    {
        _knownEventBodies = GenerateEventBodyIndex();
    }
    
    public String Encode(Type type)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));
        if (type.FullName is null) throw new InvalidOperationException("Type name cannot be null");
        return type.FullName.ToUpperInvariant();
    }

    public Type? TryDecode(String typeName)
    {
        _knownEventBodies.TryGetValue(typeName, out var value);
        return value;
    }

    private Dictionary<String, Type> GenerateEventBodyIndex()
    {
        var output = new Dictionary<String, Type>();
        var touchedAssemblies = new HashSet<String>();
        var pendingAssemblies = new Stack<Assembly>();

        Queue(AppDomain.CurrentDomain.GetAssemblies());

        while (pendingAssemblies.Count > 0)
        {
            var assembly = pendingAssemblies.Pop() ?? throw new NeverException();
            QueueByName(assembly.GetReferencedAssemblies());

            var types = assembly.GetTypes().Where(type => typeof(IEventBody).IsAssignableFrom(type) && !type.IsInterface);
            foreach (var type in types) output[Encode(type)] = type;
        }

        return output;

        void QueueByName(IEnumerable<AssemblyName> assemblyNames)
        {
            foreach (var assemblyName in assemblyNames)
            {
                var name = assemblyName.FullName;
                if (name.StartsWith("System.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (name.StartsWith("Microsoft.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (touchedAssemblies.Contains(name)) continue;
                pendingAssemblies.Push(Assembly.Load(assemblyName)); // For performance it's important that we only load an assembly we'll actually need
                touchedAssemblies.Add(name);
            }
        }

        void Queue(IEnumerable<Assembly> assemblies)
        {
            foreach (var assembly in assemblies)
            {
                var name = assembly.FullName ?? throw new NeverException();
                if (name.StartsWith("System.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (name.StartsWith("Microsoft.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (touchedAssemblies.Contains(name)) continue;

                pendingAssemblies.Push(assembly);
                touchedAssemblies.Add(name);
            }
        }
    }
}