using Application.Abstractions;
using Infrastructure.Caching.Options;
using Infrastructure.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Infrastructure.Caching;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCachingInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RedisOptions>(configuration.GetSection("Redis"));
        services.AddMemoryCache();

        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
            return ConnectionMultiplexer.Connect(options.ConnectionString);
        });

        services.AddScoped<IMessagePayloadReader>(sp =>
        {
            IMessagePayloadReader reader = sp.GetRequiredService<PostgresPendingMessageRepository>();
            reader = new RedisMessagePayloadReaderDecorator(
                reader,
                sp.GetRequiredService<IConnectionMultiplexer>(),
                sp.GetRequiredService<IOptions<Application.Options.MessageCachingOptions>>(),
                sp.GetRequiredService<ILogger<RedisMessagePayloadReaderDecorator>>());

            reader = new MemoryMessagePayloadReaderDecorator(
                reader,
                sp.GetRequiredService<Microsoft.Extensions.Caching.Memory.IMemoryCache>(),
                sp.GetRequiredService<IOptions<Application.Options.MessageCachingOptions>>(),
                sp.GetRequiredService<ILogger<MemoryMessagePayloadReaderDecorator>>());

            return reader;
        });

        services.AddScoped<IMessagePayloadCacheCoordinator, CompositeMessagePayloadCacheCoordinator>();

        return services;
    }
}
