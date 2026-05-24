namespace Application.Options;

public sealed class ExpiredMessageCleanupOptions
{
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(7);

    public int BatchSize { get; set; } = 1000;

    public int MaxRetries { get; set; } = 3;

    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(10);
}
