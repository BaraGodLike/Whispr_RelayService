namespace Application.Abstractions;

public interface IOutboxBatchRepository
{
    Task<IOutboxPublishLease?> AcquireBatchAsync(int batchSize, CancellationToken cancellationToken);
}
