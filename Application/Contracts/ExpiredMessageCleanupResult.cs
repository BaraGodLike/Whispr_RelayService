namespace Application.Contracts;

public sealed record ExpiredMessageCleanupResult(
    int DeletedPendingCount,
    int DeletedRelatedOutboxCount,
    int DeletedPublishedOutboxCount,
    int BatchCount)
{
    public int DeletedOutboxCount => DeletedRelatedOutboxCount + DeletedPublishedOutboxCount;
}
