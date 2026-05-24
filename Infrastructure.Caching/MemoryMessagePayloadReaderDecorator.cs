using Application.Abstractions;
using Application.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Infrastructure.Caching;

public sealed class MemoryMessagePayloadReaderDecorator(
    IMessagePayloadReader inner,
    IMemoryCache memoryCache,
    IOptions<MessageCachingOptions> options,
    ILogger<MemoryMessagePayloadReaderDecorator> logger) : IMessagePayloadReader
{
    private readonly MessageCachingOptions _options = options.Value;

    public async Task<byte[]?> GetPayloadAsync(Guid msgId, CancellationToken cancellationToken)
    {
        if (memoryCache.TryGetValue(CacheKeys.Message(msgId), out byte[]? payload))
        {
            return payload;
        }

        payload = await inner.GetPayloadAsync(msgId, cancellationToken);

        if (payload is null)
        {
            return null;
        }

        try
        {
            memoryCache.Set(CacheKeys.Message(msgId), payload, _options.MemoryTtl);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                "In-memory cache set failed. Layer: {Layer}, ExceptionType: {ExceptionType}.",
                nameof(MemoryMessagePayloadReaderDecorator),
                exception.GetType().FullName);
        }

        return payload;
    }
}
