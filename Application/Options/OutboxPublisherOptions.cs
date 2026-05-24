namespace Application.Options;

public sealed class OutboxPublisherOptions
{
    public int OutboxBatchSize { get; set; } = 100;

    public TimeSpan PollIntervalWhenWorkFound { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan PollIntervalWhenEmpty { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan ErrorBackoffInitial { get; set; } = TimeSpan.FromMilliseconds(500);

    public TimeSpan ErrorBackoffMax { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
