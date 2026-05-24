using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace Infrastructure.Storage;

public sealed class PostgresHealthCheck(NpgsqlDataSource dataSource) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            return HealthCheckResult.Unhealthy("Postgres connection failed.");
        }
    }
}
