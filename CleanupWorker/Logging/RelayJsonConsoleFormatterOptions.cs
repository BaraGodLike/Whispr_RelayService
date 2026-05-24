using Microsoft.Extensions.Logging.Console;

namespace CleanupWorker.Logging;

public sealed class RelayJsonConsoleFormatterOptions : ConsoleFormatterOptions
{
    public string ServiceName { get; set; } = "CleanupWorker";

    public string? InstanceId { get; set; }
}
