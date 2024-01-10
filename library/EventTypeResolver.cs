using System.Collections.Concurrent;
using System.Reflection;

// ReSharper disable MemberCanBeMadeStatic.Global

namespace ServcoX.EventSauce;

public sealed class EventTypeResolver
{
    public static readonly EventTypeResolver Shared = new();
    private readonly ConcurrentDictionary<String, Type> _knownEventBodies = new();

    public String Encode(Type type)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));
        if (type.FullName is null) throw new InvalidOperationException("Type name cannot be null");
        return type.FullName.ToUpperInvariant();
    }

    public Type? TryDecode(String typeName)
    {
        if (!_knownEventBodies.TryGetValue(typeName, out var value))
        {
            var touchedAssemblies = new HashSet<String>();
            var pendingAssemblies = new Stack<Assembly>();

            Queue(AppDomain.CurrentDomain.GetAssemblies());

            while (pendingAssemblies.Count > 0)
            {
                var assembly = pendingAssemblies.Pop() ?? throw new NeverException();
                QueueByName(assembly.GetReferencedAssemblies());

                var type = assembly.GetTypes().FirstOrDefault(type => !type.IsInterface 
                                                                      && type.FullName is not null 
                                                                      && type.FullName.Equals(typeName, StringComparison.OrdinalIgnoreCase));

                if (type != default)
                {
                    value = _knownEventBodies[typeName] = type;
                    break;
                }
            }

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

        return value;
    }
}