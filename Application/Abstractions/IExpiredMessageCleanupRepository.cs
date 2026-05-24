using Application.Contracts;

namespace Application.Abstractions;

public interface IExpiredMessageCleanupRepository
{
    Task<ExpiredMessageCleanupBatchResult> DeleteExpiredBatchAsync(
        DateTimeOffset cutoffUtc,
        int batchSize,
        CancellationToken cancellationToken);
}
