namespace Domain;

public sealed record OutboxEvent(
    Guid EventId,
    string EventType,
    Guid MsgId,
    Guid DestMailbox,
    DateTimeOffset CreatedAt,
    bool Published,
    DateTimeOffset? PublishedAt)
{
    public static OutboxEvent CreateMessageEnqueued(Guid msgId, Guid destMailbox) =>
        new(Guid.NewGuid(), RelayConstraints.MessageEnqueuedEventType, msgId, destMailbox, DateTimeOffset.MinValue, false, null);
}
