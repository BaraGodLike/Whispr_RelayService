namespace Application.Contracts;

public sealed record PendingMessageItem(Guid MsgId, Guid DestMailbox, byte[] Payload);
