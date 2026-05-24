namespace Infrastructure.Caching.Options;

public sealed class RedisOptions
{
    public string ConnectionString { get; set; } = string.Empty;
}
