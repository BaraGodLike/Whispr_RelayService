namespace Application.Options;

public sealed class MessageCachingOptions
{
    public TimeSpan MemoryTtl { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan RedisTtl { get; set; } = TimeSpan.FromMinutes(10);
}
