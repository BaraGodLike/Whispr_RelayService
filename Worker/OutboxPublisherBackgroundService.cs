using Application.Abstractions;
using Application.Contracts;
using Application.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Worker;

public sealed class OutboxPublisherBackgroundService(
    IServiceScopeFactory serviceScopeFactory,
    IOptions<OutboxPublisherOptions> options,
    ILogger<OutboxPublisherBackgroundService> logger) : BackgroundService
{
    private readonly OutboxPublisherOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var currentBackoff = _options.ErrorBackoffInitial;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var scope = serviceScopeFactory.CreateAsyncScope();
                var outboxPublisherService = scope.ServiceProvider.GetRequiredService<IOutboxPublisherService>();
                var cycleResult = await outboxPublisherService.PublishNextBatchAsync(stoppingToken);
                currentBackoff = _options.ErrorBackoffInitial;

                var nextDelay = cycleResult switch
                {
                    OutboxPublishCycleResult.PublishedWork => _options.PollIntervalWhenWorkFound,
                    _ => _options.PollIntervalWhenEmpty
                };

                await Task.Delay(nextDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                logger.LogError(
                    "Outbox publisher cycle failed. Service: {Service}, ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}.",
                    nameof(OutboxPublisherBackgroundService),
                    exception.GetType().FullName,
                    exception.Message);

                await Task.Delay(currentBackoff, stoppingToken);
                currentBackoff = NextBackoff(currentBackoff);
            }
        }
    }

    private TimeSpan NextBackoff(TimeSpan currentBackoff)
    {
        var nextMilliseconds = Math.Min(currentBackoff.TotalMilliseconds * 2, _options.ErrorBackoffMax.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(nextMilliseconds);
    }
}
