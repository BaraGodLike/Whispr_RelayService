using Domain;

namespace Application.Abstractions;

public interface IPendingMessageQueryRepository
{
    Task<IReadOnlyList<PendingMessage>> GetPendingMessagesAsync(
        IReadOnlyCollection<Guid> mailboxIds,
        int limit,
        CancellationToken cancellationToken);
}
