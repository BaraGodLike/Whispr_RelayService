using Application.Abstractions;
using Application.Contracts;
using Dapper;
using Domain;
using Infrastructure.Storage.Options;
using Npgsql;

namespace Infrastructure.Storage;

public sealed class PostgresPendingMessageRepository(
    NpgsqlDataSource dataSource) : IPendingMessageStore, IPendingMessageQueryRepository, IMessagePayloadReader, IOutboxBatchRepository, IExpiredMessageCleanupRepository
{
    public async Task<EnqueueStoreResult> EnqueueAsync(
        PendingMessage pendingMessage,
        OutboxEvent outboxEvent,
        CancellationToken cancellationToken)
    {
        const string insertPendingMessageSql =
            """
            insert into pending_messages (msg_id, dest_mailbox, payload)
            values (@MsgId, @DestMailbox, @Payload)
            on conflict do nothing;
            """;

        const string insertOutboxEventSql =
            """
            insert into outbox_events (event_id, event_type, msg_id, dest_mailbox, published)
            values (@EventId, @EventType, @MsgId, @DestMailbox, false)
            on conflict (event_type, msg_id) do nothing;
            """;

        const string getExistingPayloadSql =
            """
            select payload
            from pending_messages
            where msg_id = @MsgId;
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var insertedRows = await connection.ExecuteAsync(
            new CommandDefinition(insertPendingMessageSql, pendingMessage, transaction, cancellationToken: cancellationToken));

        if (insertedRows == 1)
        {
            await connection.ExecuteAsync(
                new CommandDefinition(insertOutboxEventSql, outboxEvent, transaction, cancellationToken: cancellationToken));

            await transaction.CommitAsync(cancellationToken);
            return EnqueueStoreResult.Inserted;
        }

        var existingPayload = await connection.QuerySingleOrDefaultAsync<byte[]>(
            new CommandDefinition(getExistingPayloadSql, new { pendingMessage.MsgId }, transaction, cancellationToken: cancellationToken));

        if (existingPayload is null)
        {
            throw new InvalidOperationException("Existing pending message payload was not found after duplicate detection.");
        }

        await transaction.CommitAsync(cancellationToken);

        return existingPayload.SequenceEqual(pendingMessage.Payload)
            ? EnqueueStoreResult.DuplicateWithSamePayload
            : EnqueueStoreResult.DuplicateWithDifferentPayload;
    }

    public async Task<bool> DeleteAsync(Guid msgId, CancellationToken cancellationToken)
    {
        const string deletePublishedOutboxSql =
            """
            delete from outbox_events
            where msg_id = @MsgId
              and published = true;
            """;

        const string deletePendingSql =
            """
            delete from pending_messages
            where msg_id = @MsgId;
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await connection.ExecuteAsync(
            new CommandDefinition(deletePublishedOutboxSql, new { MsgId = msgId }, transaction, cancellationToken: cancellationToken));

        var affectedRows = await connection.ExecuteAsync(
            new CommandDefinition(deletePendingSql, new { MsgId = msgId }, transaction, cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        return affectedRows > 0;
    }

    public async Task<int> DeleteBatchAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken)
    {
        const string deletePublishedOutboxSql =
            """
            delete from outbox_events
            where msg_id = any(@MsgIds)
              and published = true;
            """;

        const string deletePendingSql =
            """
            delete from pending_messages
            where msg_id = any(@MsgIds);
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var ids = msgIds.ToArray();

        await connection.ExecuteAsync(
            new CommandDefinition(deletePublishedOutboxSql, new { MsgIds = ids }, transaction, cancellationToken: cancellationToken));

        var deletedCount = await connection.ExecuteAsync(
            new CommandDefinition(deletePendingSql, new { MsgIds = ids }, transaction, cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        return deletedCount;
    }

    public async Task<IReadOnlyList<PendingMessage>> GetPendingMessagesAsync(
        IReadOnlyCollection<Guid> mailboxIds,
        int limit,
        CancellationToken cancellationToken)
    {
        const string sql =
            """
            select
                msg_id as MsgId,
                dest_mailbox as DestMailbox,
                payload as Payload,
                created_at as CreatedAt
            from pending_messages
            where dest_mailbox = any(@MailboxIds)
            order by created_at, msg_id
            limit @Limit;
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var storageRows = await connection.QueryAsync<PendingMessageRow>(
            new CommandDefinition(sql, new { MailboxIds = mailboxIds.ToArray(), Limit = limit }, cancellationToken: cancellationToken));

        return storageRows.Select(static row => row.ToDomain()).ToArray();
    }

    public async Task<byte[]?> GetPayloadAsync(Guid msgId, CancellationToken cancellationToken)
    {
        const string sql =
            """
            select payload
            from pending_messages
            where msg_id = @MsgId;
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<byte[]>(
            new CommandDefinition(sql, new { MsgId = msgId }, cancellationToken: cancellationToken));
    }

    public async Task<IOutboxPublishLease?> AcquireBatchAsync(int batchSize, CancellationToken cancellationToken)
    {
        const string sql =
            """
            select
                event_id as EventId,
                event_type as EventType,
                msg_id as MsgId,
                dest_mailbox as DestMailbox,
                created_at as CreatedAt,
                published as Published,
                published_at as PublishedAt
            from outbox_events
            where published = false
            order by created_at, event_id
            limit @Limit
            for update skip locked;
            """;

        var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var events = (await connection.QueryAsync<OutboxEventRow>(
                    new CommandDefinition(sql, new { Limit = batchSize }, transaction, cancellationToken: cancellationToken)))
                .Select(static row => row.ToDomain())
                .ToArray();

            return events.Length == 0
                ? await DisposeEmptyLeaseAsync(connection, transaction)
                : new PostgresOutboxPublishLease(connection, transaction, events);
        }
        catch
        {
            await transaction.DisposeAsync();
            await connection.DisposeAsync();
            throw;
        }
    }

    public async Task<ExpiredMessageCleanupBatchResult> DeleteExpiredBatchAsync(
        DateTimeOffset cutoffUtc,
        int batchSize,
        CancellationToken cancellationToken)
    {
        const string selectPublishedOutboxBatchSql =
            """
            select event_id
            from outbox_events
            where published = true
              and created_at < @CutoffUtc
            order by created_at, event_id
            limit @BatchSize
            for update skip locked;
            """;

        const string deletePublishedOutboxSql =
            """
            delete from outbox_events
            where event_id = any(@EventIds);
            """;

        const string selectBatchSql =
            """
            select msg_id
            from pending_messages
            where created_at < @CutoffUtc
            order by created_at, msg_id
            limit @BatchSize
            for update skip locked;
            """;

        const string deleteOutboxSql =
            """
            delete from outbox_events
            where msg_id = any(@MsgIds);
            """;

        const string deletePendingSql =
            """
            delete from pending_messages
            where msg_id = any(@MsgIds);
            """;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var publishedOutboxEventIds = (await connection.QueryAsync<Guid>(
                new CommandDefinition(
                    selectPublishedOutboxBatchSql,
                    new
                    {
                        CutoffUtc = cutoffUtc.UtcDateTime,
                        BatchSize = batchSize
                    },
                    transaction,
                    cancellationToken: cancellationToken)))
            .ToArray();

        var deletedPublishedOutboxCount = publishedOutboxEventIds.Length == 0
            ? 0
            : await connection.ExecuteAsync(
                new CommandDefinition(
                    deletePublishedOutboxSql,
                    new { EventIds = publishedOutboxEventIds },
                    transaction,
                    cancellationToken: cancellationToken));

        var msgIds = (await connection.QueryAsync<Guid>(
                new CommandDefinition(
                    selectBatchSql,
                    new
                    {
                        CutoffUtc = cutoffUtc.UtcDateTime,
                        BatchSize = batchSize
                    },
                    transaction,
                    cancellationToken: cancellationToken)))
            .ToArray();

        if (msgIds.Length == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return new ExpiredMessageCleanupBatchResult(
                0,
                0,
                0,
                publishedOutboxEventIds.Length,
                deletedPublishedOutboxCount);
        }

        var deletedRelatedOutboxCount = await connection.ExecuteAsync(
            new CommandDefinition(deleteOutboxSql, new { MsgIds = msgIds }, transaction, cancellationToken: cancellationToken));

        var deletedPendingCount = await connection.ExecuteAsync(
            new CommandDefinition(deletePendingSql, new { MsgIds = msgIds }, transaction, cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);

        return new ExpiredMessageCleanupBatchResult(
            msgIds.Length,
            deletedPendingCount,
            deletedRelatedOutboxCount,
            publishedOutboxEventIds.Length,
            deletedPublishedOutboxCount);
    }

    private static async Task<IOutboxPublishLease?> DisposeEmptyLeaseAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction)
    {
        await transaction.DisposeAsync();
        await connection.DisposeAsync();
        return null;
    }

    private sealed record PendingMessageRow(
        Guid MsgId,
        Guid DestMailbox,
        byte[] Payload,
        DateTime CreatedAt)
    {
        public PendingMessage ToDomain() =>
            new(MsgId, DestMailbox, Payload, ToUtcOffset(CreatedAt));
    }

    private sealed record OutboxEventRow(
        Guid EventId,
        string EventType,
        Guid MsgId,
        Guid DestMailbox,
        DateTime CreatedAt,
        bool Published,
        DateTime? PublishedAt)
    {
        public OutboxEvent ToDomain() =>
            new(
                EventId,
                EventType,
                MsgId,
                DestMailbox,
                ToUtcOffset(CreatedAt),
                Published,
                PublishedAt is null ? null : ToUtcOffset(PublishedAt.Value));
    }

    private static DateTimeOffset ToUtcOffset(DateTime value) =>
        new(DateTime.SpecifyKind(value, DateTimeKind.Utc));
}
