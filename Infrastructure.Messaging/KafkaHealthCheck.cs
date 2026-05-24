using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Infrastructure.Messaging;

public sealed class KafkaHealthCheck(IAdminClient adminClient) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(2));

            return Task.FromResult(
                metadata.Brokers.Count > 0
                    ? HealthCheckResult.Healthy()
                    : HealthCheckResult.Unhealthy("Kafka broker metadata was empty."));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka broker metadata request failed."));
        }
    }
}
