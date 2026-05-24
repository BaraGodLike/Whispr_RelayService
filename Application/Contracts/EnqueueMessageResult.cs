namespace Application.Contracts;

public enum EnqueueMessageStatus
{
    Accepted = 0,
    DuplicatePayloadMismatch = 1
}

public sealed record EnqueueMessageResult(EnqueueMessageStatus Status)
{
    public static EnqueueMessageResult Accepted() => new(EnqueueMessageStatus.Accepted);

    public static EnqueueMessageResult DuplicatePayloadMismatch() => new(EnqueueMessageStatus.DuplicatePayloadMismatch);
}
