using Application.Abstractions;
using Application.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Infrastructure.Caching;

public sealed class RedisMessagePayloadReaderDecorator(
    IMessagePayloadReader inner,
    IConnectionMultiplexer connectionMultiplexer,
    IOptions<MessageCachingOptions> options,
    ILogger<RedisMessagePayloadReaderDecorator> logger) : IMessagePayloadReader
{
    private readonly MessageCachingOptions _options = options.Value;

    public async Task<byte[]?> GetPayloadAsync(Guid msgId, CancellationToken cancellationToken)
    {
        var database = connectionMultiplexer.GetDatabase();

        try
        {
            var cached = await database.StringGetAsync(CacheKeys.Message(msgId));

            if (cached.HasValue)
            {
                return cached!;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                "Redis cache read failed. Layer: {Layer}, ExceptionType: {ExceptionType}.",
                nameof(RedisMessagePayloadReaderDecorator),
                exception.GetType().FullName);
        }

        var payload = await inner.GetPayloadAsync(msgId, cancellationToken);

        if (payload is null)
        {
            return null;
        }

        try
        {
            await database.StringSetAsync(CacheKeys.Message(msgId), payload, _options.RedisTtl);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                "Redis cache set failed. Layer: {Layer}, ExceptionType: {ExceptionType}.",
                nameof(RedisMessagePayloadReaderDecorator),
                exception.GetType().FullName);
        }

        return payload;
    }
}
