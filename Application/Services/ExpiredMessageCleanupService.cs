using Application.Abstractions;
using Application.Contracts;
using Application.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Application.Services;

public sealed class ExpiredMessageCleanupService(
    IExpiredMessageCleanupRepository cleanupRepository,
    IOptions<ExpiredMessageCleanupOptions> options,
    ILogger<ExpiredMessageCleanupService> logger) : IExpiredMessageCleanupService
{
    private readonly ExpiredMessageCleanupOptions _options = options.Value;

    public async Task<ExpiredMessageCleanupResult> CleanupExpiredMessagesAsync(CancellationToken cancellationToken)
    {
        var cutoffUtc = DateTimeOffset.UtcNow - _options.Retention;
        var deletedPendingTotal = 0;
        var deletedRelatedOutboxTotal = 0;
        var deletedPublishedOutboxTotal = 0;
        var batchCount = 0;

        while (true)
        {
            var batchResult = await cleanupRepository.DeleteExpiredBatchAsync(
                cutoffUtc,
                _options.BatchSize,
                cancellationToken);

            if (!batchResult.HasWork)
            {
                return new ExpiredMessageCleanupResult(
                    deletedPendingTotal,
                    deletedRelatedOutboxTotal,
                    deletedPublishedOutboxTotal,
                    batchCount);
            }

            batchCount++;
            deletedPendingTotal += batchResult.DeletedPendingCount;
            deletedRelatedOutboxTotal += batchResult.DeletedRelatedOutboxCount;
            deletedPublishedOutboxTotal += batchResult.DeletedPublishedOutboxCount;

            logger.LogInformation(
                "Expired message cleanup batch completed. BatchNumber: {BatchNumber}, DeletedPendingCount: {DeletedPendingCount}, DeletedRelatedOutboxCount: {DeletedRelatedOutboxCount}, DeletedPublishedOutboxCount: {DeletedPublishedOutboxCount}, DeletedOutboxCount: {DeletedOutboxCount}.",
                batchCount,
                batchResult.DeletedPendingCount,
                batchResult.DeletedRelatedOutboxCount,
                batchResult.DeletedPublishedOutboxCount,
                batchResult.DeletedOutboxCount);
        }
    }
}
