namespace Application.Abstractions;

public interface IMessagePayloadReader
{
    Task<byte[]?> GetPayloadAsync(Guid msgId, CancellationToken cancellationToken);
}
