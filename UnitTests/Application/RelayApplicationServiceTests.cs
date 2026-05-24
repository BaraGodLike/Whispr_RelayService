using Application.Abstractions;
using Application.Contracts;
using Application.Services;
using Domain;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace UnitTests.Application;

[TestClass]
public sealed class RelayApplicationServiceTests
{
    private IPendingMessageStore _pendingMessageStore = null!;
    private IPendingMessageQueryRepository _pendingMessageQueryRepository = null!;
    private IMessagePayloadReader _messagePayloadReader = null!;
    private IMessagePayloadCacheCoordinator _messagePayloadCacheCoordinator = null!;
    private RelayApplicationService _service = null!;

    [TestInitialize]
    public void Initialize()
    {
        _pendingMessageStore = Substitute.For<IPendingMessageStore>();
        _pendingMessageQueryRepository = Substitute.For<IPendingMessageQueryRepository>();
        _messagePayloadReader = Substitute.For<IMessagePayloadReader>();
        _messagePayloadCacheCoordinator = Substitute.For<IMessagePayloadCacheCoordinator>();

        _service = new RelayApplicationService(
            _pendingMessageStore,
            _pendingMessageQueryRepository,
            _messagePayloadReader,
            _messagePayloadCacheCoordinator,
            NullLogger<RelayApplicationService>.Instance);
    }

    [TestMethod]
    public async Task EnqueueAsync_WhenStoreReportsInserted_ReturnsAccepted()
    {
        var msgId = Guid.NewGuid();
        var mailbox = Guid.NewGuid();
        var payload = new byte[] { 1, 2, 3 };

        _pendingMessageStore
            .EnqueueAsync(Arg.Any<PendingMessage>(), Arg.Any<OutboxEvent>(), Arg.Any<CancellationToken>())
            .Returns(EnqueueStoreResult.Inserted);

        _messagePayloadCacheCoordinator
            .TryWarmAsync(msgId, payload, Arg.Any<CancellationToken>())
            .Returns(true);

        var result = await _service.EnqueueAsync(msgId, mailbox, payload, CancellationToken.None);

        Assert.AreEqual(EnqueueMessageStatus.Accepted, result.Status);
        await _messagePayloadCacheCoordinator.Received(1).TryWarmAsync(msgId, payload, Arg.Any<CancellationToken>());
    }

    [TestMethod]
    public async Task EnqueueAsync_WhenStoreReportsPayloadMismatch_ReturnsMismatch()
    {
        _pendingMessageStore
            .EnqueueAsync(Arg.Any<PendingMessage>(), Arg.Any<OutboxEvent>(), Arg.Any<CancellationToken>())
            .Returns(EnqueueStoreResult.DuplicateWithDifferentPayload);

        var result = await _service.EnqueueAsync(Guid.NewGuid(), Guid.NewGuid(), new byte[] { 7 }, CancellationToken.None);

        Assert.AreEqual(EnqueueMessageStatus.DuplicatePayloadMismatch, result.Status);
        await _messagePayloadCacheCoordinator.DidNotReceiveWithAnyArgs().TryWarmAsync(default, default!, default);
    }

    [TestMethod]
    public async Task GetPendingMessagesAsync_WhenMoreMessagesExist_SetsHasMore()
    {
        var first = PendingMessage.Create(Guid.NewGuid(), Guid.NewGuid(), new byte[] { 1 }) with { CreatedAt = DateTimeOffset.UtcNow };
        var second = PendingMessage.Create(Guid.NewGuid(), Guid.NewGuid(), new byte[] { 2 }) with { CreatedAt = DateTimeOffset.UtcNow.AddSeconds(1) };
        var third = PendingMessage.Create(Guid.NewGuid(), Guid.NewGuid(), new byte[] { 3 }) with { CreatedAt = DateTimeOffset.UtcNow.AddSeconds(2) };

        _pendingMessageQueryRepository
            .GetPendingMessagesAsync(Arg.Any<IReadOnlyCollection<Guid>>(), 3, Arg.Any<CancellationToken>())
            .Returns(new[] { first, second, third });

        var result = await _service.GetPendingMessagesAsync(new[] { Guid.NewGuid() }, 2, CancellationToken.None);

        Assert.IsTrue(result.HasMore);
        Assert.HasCount(2, result.Messages);
        CollectionAssert.AreEqual(first.Payload, result.Messages[0].Payload);
        CollectionAssert.AreEqual(second.Payload, result.Messages[1].Payload);
    }

    [TestMethod]
    public async Task AckMessagesBatchAsync_DeduplicatesIdsBeforeDelete()
    {
        var msgId = Guid.NewGuid();

        _pendingMessageStore
            .DeleteBatchAsync(Arg.Any<IReadOnlyCollection<Guid>>(), Arg.Any<CancellationToken>())
            .Returns(1);

        _messagePayloadCacheCoordinator
            .TryEvictManyAsync(Arg.Any<IReadOnlyCollection<Guid>>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var ackedCount = await _service.AckMessagesBatchAsync(new[] { msgId, msgId }, CancellationToken.None);

        Assert.AreEqual(1, ackedCount);

        await _pendingMessageStore.Received(1).DeleteBatchAsync(
            Arg.Is<IReadOnlyCollection<Guid>>(ids => ids.Count == 1 && ids.Single() == msgId),
            Arg.Any<CancellationToken>());

        await _messagePayloadCacheCoordinator.Received(1).TryEvictManyAsync(
            Arg.Is<IReadOnlyCollection<Guid>>(ids => ids.Count == 1 && ids.Single() == msgId),
            Arg.Any<CancellationToken>());
    }
}
