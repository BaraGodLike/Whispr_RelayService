namespace Application.Contracts;

public sealed record GetPendingMessagesResult(IReadOnlyList<PendingMessageItem> Messages, bool HasMore);
