using Domain;

namespace Application.Abstractions;

public interface IOutboxPublishLease : IAsyncDisposable
{
    IReadOnlyList<OutboxEvent> Events { get; }

    Task MarkPublishedAsync(Guid eventId, DateTimeOffset publishedAt, CancellationToken cancellationToken);

    Task CommitAsync(CancellationToken cancellationToken);
}
