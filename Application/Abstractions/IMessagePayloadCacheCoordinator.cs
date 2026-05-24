namespace Application.Abstractions;

public interface IMessagePayloadCacheCoordinator
{
    Task<bool> TryWarmAsync(Guid msgId, byte[] payload, CancellationToken cancellationToken);

    Task<bool> TryEvictAsync(Guid msgId, CancellationToken cancellationToken);

    Task<bool> TryEvictManyAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken);
}
