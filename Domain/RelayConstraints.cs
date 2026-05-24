namespace Domain;

public static class RelayConstraints
{
    public const int MaxPayloadBytes = 256 * 1024;
    public const int DefaultPendingMessagesLimit = 500;
    public const int MaxPendingMessagesLimit = 500;
    public const int MaxMailboxCount = 7;
    public const int MaxAckBatchSize = 500;
    public const string MessageEnqueuedEventType = "message.enqueued";
}
