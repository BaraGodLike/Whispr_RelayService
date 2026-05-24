using Application.Abstractions;
using Application.Options;
using Application.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Application;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationServices(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MessageCachingOptions>(configuration.GetSection("Caching"));
        services.Configure<OutboxPublisherOptions>(configuration.GetSection("OutboxPublisher"));

        services.AddScoped<IRelayApplicationService, RelayApplicationService>();
        services.AddScoped<IOutboxPublisherService, OutboxPublisherService>();

        return services;
    }
}
