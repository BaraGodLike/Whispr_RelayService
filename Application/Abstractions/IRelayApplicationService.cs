using Application.Contracts;

namespace Application.Abstractions;

public interface IRelayApplicationService
{
    Task<EnqueueMessageResult> EnqueueAsync(Guid msgId, Guid destMailbox, byte[] payload, CancellationToken cancellationToken);

    Task<byte[]?> GetMessageAsync(Guid msgId, CancellationToken cancellationToken);

    Task<GetPendingMessagesResult> GetPendingMessagesAsync(
        IReadOnlyCollection<Guid> mailboxIds,
        int limit,
        CancellationToken cancellationToken);

    Task AckMessageAsync(Guid msgId, CancellationToken cancellationToken);

    Task<int> AckMessagesBatchAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken);
}
