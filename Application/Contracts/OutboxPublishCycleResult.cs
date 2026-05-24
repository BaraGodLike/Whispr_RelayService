namespace Application.Contracts;

public enum OutboxPublishCycleResult
{
    NoWork = 0,
    PublishedWork = 1
}
