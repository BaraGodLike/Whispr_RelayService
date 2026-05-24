using Domain;

namespace Application.Abstractions;

public interface IMessageEnqueuedEventPublisher
{
    Task PublishAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken);
}
