namespace Application.Contracts;

public sealed record ExpiredMessageCleanupBatchResult(
    int SelectedPendingCount,
    int DeletedPendingCount,
    int DeletedRelatedOutboxCount,
    int SelectedPublishedOutboxCount,
    int DeletedPublishedOutboxCount)
{
    public int DeletedOutboxCount => DeletedRelatedOutboxCount + DeletedPublishedOutboxCount;

    public bool HasWork => SelectedPendingCount > 0 || SelectedPublishedOutboxCount > 0;
}
