using Microsoft.Extensions.Logging.Console;

namespace RelayService.Logging;

public sealed class RelayJsonConsoleFormatterOptions : ConsoleFormatterOptions
{
    public string ServiceName { get; set; } = "RelayService";

    public string? InstanceId { get; set; }
}
