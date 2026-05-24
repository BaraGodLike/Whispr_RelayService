using Application.Contracts;

namespace Application.Abstractions;

public interface IOutboxPublisherService
{
    Task<OutboxPublishCycleResult> PublishNextBatchAsync(CancellationToken cancellationToken);
}
