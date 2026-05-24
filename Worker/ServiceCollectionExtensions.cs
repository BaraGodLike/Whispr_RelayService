using Microsoft.Extensions.DependencyInjection;

namespace Worker;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddWorkerServices(this IServiceCollection services)
    {
        services.AddHostedService<OutboxPublisherBackgroundService>();
        return services;
    }
}
