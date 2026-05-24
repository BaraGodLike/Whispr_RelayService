using Domain;

namespace Application.Abstractions;

public interface IPendingMessageStore
{
    Task<EnqueueStoreResult> EnqueueAsync(PendingMessage pendingMessage, OutboxEvent outboxEvent, CancellationToken cancellationToken);

    Task<bool> DeleteAsync(Guid msgId, CancellationToken cancellationToken);

    Task<int> DeleteBatchAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken);
}
