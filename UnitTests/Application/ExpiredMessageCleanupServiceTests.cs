using Application.Abstractions;
using Application.Contracts;
using Application.Options;
using Application.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace UnitTests.Application;

[TestClass]
public sealed class ExpiredMessageCleanupServiceTests
{
    [TestMethod]
    public async Task CleanupExpiredMessagesAsync_ProcessesBatchesUntilRepositoryReturnsEmptyBatch()
    {
        var repository = Substitute.For<IExpiredMessageCleanupRepository>();
        var options = Options.Create(new ExpiredMessageCleanupOptions
        {
            Retention = TimeSpan.FromDays(7),
            BatchSize = 1000
        });

        repository.DeleteExpiredBatchAsync(Arg.Any<DateTimeOffset>(), 1000, Arg.Any<CancellationToken>())
            .Returns(
                new ExpiredMessageCleanupBatchResult(1000, 1000, 1000, 1000, 1000),
                new ExpiredMessageCleanupBatchResult(1000, 1000, 900, 300, 300),
                new ExpiredMessageCleanupBatchResult(0, 0, 0, 0, 0));

        var service = new ExpiredMessageCleanupService(
            repository,
            options,
            NullLogger<ExpiredMessageCleanupService>.Instance);

        var result = await service.CleanupExpiredMessagesAsync(CancellationToken.None);

        Assert.AreEqual(2000, result.DeletedPendingCount);
        Assert.AreEqual(1900, result.DeletedRelatedOutboxCount);
        Assert.AreEqual(1300, result.DeletedPublishedOutboxCount);
        Assert.AreEqual(3200, result.DeletedOutboxCount);
        Assert.AreEqual(2, result.BatchCount);

        await repository.Received(3).DeleteExpiredBatchAsync(
            Arg.Any<DateTimeOffset>(),
            1000,
            Arg.Any<CancellationToken>());
    }

    [TestMethod]
    public async Task CleanupExpiredMessagesAsync_ContinuesWhenOnlyPublishedOutboxRowsRemain()
    {
        var repository = Substitute.For<IExpiredMessageCleanupRepository>();
        var options = Options.Create(new ExpiredMessageCleanupOptions
        {
            Retention = TimeSpan.FromDays(7),
            BatchSize = 1000
        });

        repository.DeleteExpiredBatchAsync(Arg.Any<DateTimeOffset>(), 1000, Arg.Any<CancellationToken>())
            .Returns(
                new ExpiredMessageCleanupBatchResult(0, 0, 0, 1000, 1000),
                new ExpiredMessageCleanupBatchResult(0, 0, 0, 0, 0));

        var service = new ExpiredMessageCleanupService(
            repository,
            options,
            NullLogger<ExpiredMessageCleanupService>.Instance);

        var result = await service.CleanupExpiredMessagesAsync(CancellationToken.None);

        Assert.AreEqual(0, result.DeletedPendingCount);
        Assert.AreEqual(0, result.DeletedRelatedOutboxCount);
        Assert.AreEqual(1000, result.DeletedPublishedOutboxCount);
        Assert.AreEqual(1000, result.DeletedOutboxCount);
        Assert.AreEqual(1, result.BatchCount);

        await repository.Received(2).DeleteExpiredBatchAsync(
            Arg.Any<DateTimeOffset>(),
            1000,
            Arg.Any<CancellationToken>());
    }
}
