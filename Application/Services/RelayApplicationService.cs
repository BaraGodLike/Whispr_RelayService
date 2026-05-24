using Application.Abstractions;
using Application.Contracts;
using Microsoft.Extensions.Logging;
using Domain;

namespace Application.Services;

public sealed class RelayApplicationService(
    IPendingMessageStore pendingMessageStore,
    IPendingMessageQueryRepository pendingMessageQueryRepository,
    IMessagePayloadReader messagePayloadReader,
    IMessagePayloadCacheCoordinator messagePayloadCacheCoordinator,
    ILogger<RelayApplicationService> logger) : IRelayApplicationService
{
    public async Task<EnqueueMessageResult> EnqueueAsync(
        Guid msgId,
        Guid destMailbox,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        var pendingMessage = PendingMessage.Create(msgId, destMailbox, payload);
        var outboxEvent = OutboxEvent.CreateMessageEnqueued(msgId, destMailbox);

        var storeResult = await pendingMessageStore.EnqueueAsync(pendingMessage, outboxEvent, cancellationToken);

        switch (storeResult)
        {
            case EnqueueStoreResult.Inserted:
            case EnqueueStoreResult.DuplicateWithSamePayload:
                await WarnIfCacheWarmupFailedAsync(msgId, payload, cancellationToken);
                return EnqueueMessageResult.Accepted();
            case EnqueueStoreResult.DuplicateWithDifferentPayload:
                return EnqueueMessageResult.DuplicatePayloadMismatch();
            default:
                throw new InvalidOperationException("Unknown enqueue store result.");
        }
    }

    public Task<byte[]?> GetMessageAsync(Guid msgId, CancellationToken cancellationToken) =>
        messagePayloadReader.GetPayloadAsync(msgId, cancellationToken);

    public async Task<GetPendingMessagesResult> GetPendingMessagesAsync(
        IReadOnlyCollection<Guid> mailboxIds,
        int limit,
        CancellationToken cancellationToken)
    {
        var messages = await pendingMessageQueryRepository.GetPendingMessagesAsync(mailboxIds, limit + 1, cancellationToken);
        var hasMore = messages.Count > limit;
        var items = messages
            .Take(limit)
            .Select(message => new PendingMessageItem(message.MsgId, message.DestMailbox, message.Payload))
            .ToArray();

        return new GetPendingMessagesResult(items, hasMore);
    }

    public async Task AckMessageAsync(Guid msgId, CancellationToken cancellationToken)
    {
        await pendingMessageStore.DeleteAsync(msgId, cancellationToken);
        await WarnIfCacheEvictionFailedAsync(msgId, cancellationToken);
    }

    public async Task<int> AckMessagesBatchAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken)
    {
        var uniqueIds = msgIds.Distinct().ToArray();
        var deletedCount = await pendingMessageStore.DeleteBatchAsync(uniqueIds, cancellationToken);
        await WarnIfBatchCacheEvictionFailedAsync(uniqueIds, cancellationToken);
        return deletedCount;
    }

    private async Task WarnIfCacheWarmupFailedAsync(Guid msgId, byte[] payload, CancellationToken cancellationToken)
    {
        var warmed = await messagePayloadCacheCoordinator.TryWarmAsync(msgId, payload, cancellationToken);

        if (!warmed)
        {
            logger.LogWarning(
                "Relay cache warmup failed. Operation: {Operation}.",
                nameof(EnqueueAsync));
        }
    }

    private async Task WarnIfCacheEvictionFailedAsync(Guid msgId, CancellationToken cancellationToken)
    {
        var evicted = await messagePayloadCacheCoordinator.TryEvictAsync(msgId, cancellationToken);

        if (!evicted)
        {
            logger.LogWarning(
                "Relay cache eviction failed. Operation: {Operation}.",
                nameof(AckMessageAsync));
        }
    }

    private async Task WarnIfBatchCacheEvictionFailedAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken)
    {
        var evicted = await messagePayloadCacheCoordinator.TryEvictManyAsync(msgIds, cancellationToken);

        if (!evicted)
        {
            logger.LogWarning(
                "Relay batch cache eviction failed. Operation: {Operation}.",
                nameof(AckMessagesBatchAsync));
        }
    }
}
