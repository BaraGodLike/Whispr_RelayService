using Application.Abstractions;
using Application.Contracts;
using Application.Options;
using Microsoft.Extensions.Options;

namespace Application.Services;

public sealed class OutboxPublisherService(
    IOutboxBatchRepository outboxBatchRepository,
    IMessageEnqueuedEventPublisher messageEnqueuedEventPublisher,
    IOptions<OutboxPublisherOptions> options) : IOutboxPublisherService
{
    private readonly OutboxPublisherOptions _options = options.Value;

    public async Task<OutboxPublishCycleResult> PublishNextBatchAsync(CancellationToken cancellationToken)
    {
        await using var lease = await outboxBatchRepository.AcquireBatchAsync(_options.OutboxBatchSize, cancellationToken);

        if (lease is null || lease.Events.Count == 0)
        {
            return OutboxPublishCycleResult.NoWork;
        }

        foreach (var outboxEvent in lease.Events)
        {
            using var timeoutCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCancellationTokenSource.CancelAfter(_options.PublishTimeout);

            await messageEnqueuedEventPublisher.PublishAsync(outboxEvent, timeoutCancellationTokenSource.Token);
            await lease.MarkPublishedAsync(outboxEvent.EventId, DateTimeOffset.UtcNow, cancellationToken);
        }

        await lease.CommitAsync(cancellationToken);
        return OutboxPublishCycleResult.PublishedWork;
    }
}
