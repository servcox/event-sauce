using Microsoft.Extensions.DependencyInjection;
using ServcoX.EventSauce.Configurations;

namespace ServcoX.EventSauce;

public static class Builder
{
    public static IServiceCollection AddEventSauce(this IServiceCollection target, String connectionString, Action<BaseConfiguration>? configure = null)
    {
        target.AddSingleton<IEventStore>(new EventStore(connectionString, configure));
        return target;
    }
}