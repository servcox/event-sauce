using Microsoft.Extensions.DependencyInjection;
using ServcoX.EventSauce.Configurations;
using ServcoX.EventSauce.V2;
using ServcoX.EventSauce.V2.Configurations;

namespace ServcoX.EventSauce;

public static class Builder
{
    public static IServiceCollection AddEventSauce(this IServiceCollection target, String connectionString, String containerName, String aggregateName,
        Action<EventStoreConfiguration>? configure = null)
    {
        target.AddSingleton(new EventStore(connectionString, containerName, aggregateName, configure));
        return target;
    }

    public static IServiceCollection AddOldEventSauce(this IServiceCollection target, String connectionString, Action<BaseConfiguration>? configure = null)
    {
        target.AddSingleton<IEventStore>(new V2.EventStore(connectionString, configure));
        return target;
    }
}