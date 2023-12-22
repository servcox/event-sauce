using System.Reflection;

namespace ServcoX.EventSauce.EventStores;

public sealed class EventTypeResolver
{
    private readonly IReadOnlyDictionary<String, Type> _knownEventBodies;

    public EventTypeResolver()
    {
        _knownEventBodies = GenerateEventBodyIndex();
    }
    
    public String Encode(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        if (type.FullName is null) throw new InvalidOperationException("Type name cannot be null");
        return type.FullName.ToUpperInvariant();
    }

    public Type? TryDecode(String typeName) => _knownEventBodies.GetValueOrDefault(typeName);
    
    private Dictionary<String, Type> GenerateEventBodyIndex()
    {
        var output = new Dictionary<String, Type>();
        var touchedAssemblies = new HashSet<String>();
        var pendingAssemblies = new Stack<Assembly>();

        Queue(AppDomain.CurrentDomain.GetAssemblies());

        while (pendingAssemblies.TryPop(out var assembly))
        {
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
                var name = assembly.FullName;
                if (name is null) continue;
                if (name.StartsWith("System.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (name.StartsWith("Microsoft.", StringComparison.InvariantCultureIgnoreCase)) continue;
                if (touchedAssemblies.Contains(name)) continue;

                pendingAssemblies.Push(assembly);
                touchedAssemblies.Add(name);
            }
        }
    }
}