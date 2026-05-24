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
        services.Configure<ExpiredMessageCleanupOptions>(configuration.GetSection("ExpiredMessageCleanup"));

        services.AddScoped<IRelayApplicationService, RelayApplicationService>();
        services.AddScoped<IOutboxPublisherService, OutboxPublisherService>();
        services.AddScoped<IExpiredMessageCleanupService, ExpiredMessageCleanupService>();

        return services;
    }
}
