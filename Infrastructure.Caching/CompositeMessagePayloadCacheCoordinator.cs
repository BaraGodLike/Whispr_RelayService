using Application.Abstractions;
using Application.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Infrastructure.Caching;

public sealed class CompositeMessagePayloadCacheCoordinator(
    IMemoryCache memoryCache,
    IConnectionMultiplexer connectionMultiplexer,
    IOptions<MessageCachingOptions> options) : IMessagePayloadCacheCoordinator
{
    private readonly MessageCachingOptions _options = options.Value;

    public async Task<bool> TryWarmAsync(Guid msgId, byte[] payload, CancellationToken cancellationToken)
    {
        var memorySucceeded = TryWarmMemory(msgId, payload);
        var redisSucceeded = await TryWarmRedisAsync(msgId, payload);
        return memorySucceeded && redisSucceeded;
    }

    public async Task<bool> TryEvictAsync(Guid msgId, CancellationToken cancellationToken)
    {
        var memorySucceeded = TryEvictMemory(msgId);
        var redisSucceeded = await TryEvictRedisAsync(msgId);
        return memorySucceeded && redisSucceeded;
    }

    public async Task<bool> TryEvictManyAsync(IReadOnlyCollection<Guid> msgIds, CancellationToken cancellationToken)
    {
        var results = await Task.WhenAll(msgIds.Select(msgId => TryEvictAsync(msgId, cancellationToken)));
        return results.All(static result => result);
    }

    private bool TryWarmMemory(Guid msgId, byte[] payload)
    {
        try
        {
            memoryCache.Set(CacheKeys.Message(msgId), payload, _options.MemoryTtl);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private bool TryEvictMemory(Guid msgId)
    {
        try
        {
            memoryCache.Remove(CacheKeys.Message(msgId));
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> TryWarmRedisAsync(Guid msgId, byte[] payload)
    {
        try
        {
            await connectionMultiplexer.GetDatabase().StringSetAsync(CacheKeys.Message(msgId), payload, _options.RedisTtl);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> TryEvictRedisAsync(Guid msgId)
    {
        try
        {
            await connectionMultiplexer.GetDatabase().KeyDeleteAsync(CacheKeys.Message(msgId));
            return true;
        }
        catch
        {
            return false;
        }
    }
}
