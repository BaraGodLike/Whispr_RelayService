using Microsoft.Extensions.Logging.Console;

namespace OutboxWorker.Logging;

public sealed class RelayJsonConsoleFormatterOptions : ConsoleFormatterOptions
{
    public string ServiceName { get; set; } = "OutboxWorker";

    public string? InstanceId { get; set; }
}
