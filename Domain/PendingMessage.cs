namespace Domain;

public sealed record PendingMessage(
    Guid MsgId,
    Guid DestMailbox,
    byte[] Payload,
    DateTimeOffset CreatedAt)
{
    public static PendingMessage Create(Guid msgId, Guid destMailbox, byte[] payload) =>
        new(msgId, destMailbox, payload.ToArray(), DateTimeOffset.MinValue);
}
