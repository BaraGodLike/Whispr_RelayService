using Application;
using Application.Abstractions;
using Application.Options;
using CleanupWorker.Logging;
using Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Services.Configure<RelayJsonConsoleFormatterOptions>(builder.Configuration.GetSection("ServiceMetadata"));
builder.Logging.AddConfiguration(builder.Configuration.GetSection("Logging"));
builder.Logging.AddFilter("Startup", LogLevel.Information);
builder.Logging.AddFilter("Application", LogLevel.Information);
builder.Logging.AddFilter("Infrastructure.Storage", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddConsoleFormatter<RelayJsonConsoleFormatter, RelayJsonConsoleFormatterOptions>();
builder.Logging.AddConsole(options => options.FormatterName = RelayJsonConsoleFormatter.FormatterName);

builder.Services
    .AddApplicationServices(builder.Configuration)
    .AddStorageInfrastructure(builder.Configuration);

using var host = builder.Build();

var loggerFactory = host.Services.GetRequiredService<ILoggerFactory>();
var startupLogger = loggerFactory.CreateLogger("Startup");
startupLogger.LogInformation("Cleanup worker startup completed.");

using var cancellationTokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellationTokenSource.Cancel();
};

return await RunCleanupAsync(host.Services, loggerFactory, cancellationTokenSource.Token);

static async Task<int> RunCleanupAsync(
    IServiceProvider services,
    ILoggerFactory loggerFactory,
    CancellationToken cancellationToken)
{
    await using var scope = services.CreateAsyncScope();

    var cleanupService = scope.ServiceProvider.GetRequiredService<IExpiredMessageCleanupService>();
    var options = scope.ServiceProvider.GetRequiredService<IOptions<ExpiredMessageCleanupOptions>>().Value;
    var logger = loggerFactory.CreateLogger("Application.CleanupWorker");

    var attempt = 0;
    var delay = options.InitialRetryDelay;

    while (true)
    {
        attempt++;

        try
        {
            var startedAt = DateTimeOffset.UtcNow;
            var result = await cleanupService.CleanupExpiredMessagesAsync(cancellationToken);
            var duration = DateTimeOffset.UtcNow - startedAt;

            logger.LogInformation(
                "Expired message cleanup completed. Attempt: {Attempt}, DeletedPendingCount: {DeletedPendingCount}, DeletedRelatedOutboxCount: {DeletedRelatedOutboxCount}, DeletedPublishedOutboxCount: {DeletedPublishedOutboxCount}, DeletedOutboxCount: {DeletedOutboxCount}, BatchCount: {BatchCount}, DurationMs: {DurationMs}.",
                attempt,
                result.DeletedPendingCount,
                result.DeletedRelatedOutboxCount,
                result.DeletedPublishedOutboxCount,
                result.DeletedOutboxCount,
                result.BatchCount,
                duration.TotalMilliseconds);

            return 0;
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "Expired message cleanup cancelled. Attempt: {Attempt}.",
                attempt);

            return 1;
        }
        catch (Exception exception) when (attempt < options.MaxRetries)
        {
            logger.LogError(
                "Expired message cleanup attempt failed. Attempt: {Attempt}, MaxRetries: {MaxRetries}, ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}, NextRetryDelayMs: {NextRetryDelayMs}.",
                attempt,
                options.MaxRetries,
                exception.GetType().FullName,
                exception.Message,
                delay.TotalMilliseconds);

            await Task.Delay(delay, cancellationToken);
            delay = NextDelay(delay, options.MaxRetryDelay);
        }
        catch (Exception exception)
        {
            logger.LogError(
                "Expired message cleanup failed. Attempt: {Attempt}, MaxRetries: {MaxRetries}, ExceptionType: {ExceptionType}, ExceptionMessage: {ExceptionMessage}.",
                attempt,
                options.MaxRetries,
                exception.GetType().FullName,
                exception.Message);

            return 1;
        }
    }
}

static TimeSpan NextDelay(TimeSpan currentDelay, TimeSpan maxDelay)
{
    var nextDelayMilliseconds = Math.Min(currentDelay.TotalMilliseconds * 2, maxDelay.TotalMilliseconds);
    return TimeSpan.FromMilliseconds(nextDelayMilliseconds);
}
