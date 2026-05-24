using Application.Abstractions;
using Infrastructure.Storage.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Infrastructure.Storage;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddStorageInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<PostgresOptions>(configuration.GetSection("Postgres"));

        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<PostgresOptions>>().Value;
            var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
            return builder.Build();
        });

        services.AddScoped<PostgresPendingMessageRepository>();
        services.AddScoped<IPendingMessageStore>(sp => sp.GetRequiredService<PostgresPendingMessageRepository>());
        services.AddScoped<IPendingMessageQueryRepository>(sp => sp.GetRequiredService<PostgresPendingMessageRepository>());
        services.AddScoped<IOutboxBatchRepository>(sp => sp.GetRequiredService<PostgresPendingMessageRepository>());

        return services;
    }
}
