using Application.Abstractions;
using Domain;
using Dapper;
using Npgsql;

namespace Infrastructure.Storage;

internal sealed class PostgresOutboxPublishLease(
    NpgsqlConnection connection,
    NpgsqlTransaction transaction,
    IReadOnlyList<OutboxEvent> events) : IOutboxPublishLease
{
    private bool _committed;

    public IReadOnlyList<OutboxEvent> Events { get; } = events;

    public Task MarkPublishedAsync(Guid eventId, DateTimeOffset publishedAt, CancellationToken cancellationToken)
    {
        const string sql =
            """
            update outbox_events
            set published = true,
                published_at = @PublishedAt
            where event_id = @EventId;
            """;

        return connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                EventId = eventId,
                PublishedAt = publishedAt
            },
            transaction,
            cancellationToken: cancellationToken));
    }

    public async Task CommitAsync(CancellationToken cancellationToken)
    {
        await transaction.CommitAsync(cancellationToken);
        _committed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_committed)
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }

        await transaction.DisposeAsync();
        await connection.DisposeAsync();
    }
}
