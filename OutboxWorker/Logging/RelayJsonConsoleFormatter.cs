using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;

namespace OutboxWorker.Logging;

public sealed class RelayJsonConsoleFormatter : ConsoleFormatter, IDisposable
{
    public const string FormatterName = "outbox-json";

    private readonly IDisposable? _optionsReloadToken;
    private RelayJsonConsoleFormatterOptions _options;

    public RelayJsonConsoleFormatter(IOptionsMonitor<RelayJsonConsoleFormatterOptions> options)
        : base(FormatterName)
    {
        _options = options.CurrentValue;
        _optionsReloadToken = options.OnChange(updatedOptions => _options = updatedOptions);
    }

    public override void Write<TState>(
        in LogEntry<TState> logEntry,
        IExternalScopeProvider? scopeProvider,
        TextWriter textWriter)
    {
        var message = logEntry.Formatter?.Invoke(logEntry.State, logEntry.Exception);

        if (message is null && logEntry.Exception is null)
        {
            return;
        }

        var bufferWriter = new ArrayBufferWriter<byte>();

        using (var jsonWriter = new Utf8JsonWriter(bufferWriter))
        {
            jsonWriter.WriteStartObject();
            jsonWriter.WriteString("Timestamp", DateTimeOffset.UtcNow);
            jsonWriter.WriteString("Service", ResolveServiceName());
            jsonWriter.WriteString("Instance", ResolveInstanceId());
            jsonWriter.WriteNumber("EventId", logEntry.EventId.Id);
            jsonWriter.WriteString("LogLevel", logEntry.LogLevel.ToString());
            jsonWriter.WriteString("Category", logEntry.Category);

            if (!string.IsNullOrWhiteSpace(message))
            {
                jsonWriter.WriteString("Message", message);
            }

            WriteState(jsonWriter, logEntry.State);
            WriteException(jsonWriter, logEntry.Exception);

            jsonWriter.WriteEndObject();
            jsonWriter.Flush();
        }

        textWriter.WriteLine(Encoding.UTF8.GetString(bufferWriter.WrittenSpan));
    }

    public void Dispose()
    {
        _optionsReloadToken?.Dispose();
    }

    private string ResolveServiceName() =>
        string.IsNullOrWhiteSpace(_options.ServiceName)
            ? "OutboxWorker"
            : _options.ServiceName;

    private string ResolveInstanceId()
    {
        if (!string.IsNullOrWhiteSpace(_options.InstanceId))
        {
            return _options.InstanceId;
        }

        return Environment.GetEnvironmentVariable("HOSTNAME")
               ?? Environment.GetEnvironmentVariable("COMPUTERNAME")
               ?? Environment.MachineName;
    }

    private static void WriteException(Utf8JsonWriter jsonWriter, Exception? exception)
    {
        if (exception is null)
        {
            return;
        }

        jsonWriter.WriteString("ExceptionType", exception.GetType().FullName);
        jsonWriter.WriteString("ExceptionMessage", exception.Message);
    }

    private static void WriteState<TState>(Utf8JsonWriter jsonWriter, TState state)
    {
        if (state is not IReadOnlyList<KeyValuePair<string, object?>> stateProperties || stateProperties.Count == 0)
        {
            return;
        }

        jsonWriter.WriteStartObject("State");

        foreach (var pair in stateProperties)
        {
            WriteValue(jsonWriter, pair.Key, pair.Value);
        }

        jsonWriter.WriteEndObject();
    }

    private static void WriteValue(Utf8JsonWriter jsonWriter, string name, object? value)
    {
        switch (value)
        {
            case null:
                jsonWriter.WriteNull(name);
                break;
            case bool boolValue:
                jsonWriter.WriteBoolean(name, boolValue);
                break;
            case byte byteValue:
                jsonWriter.WriteNumber(name, byteValue);
                break;
            case short shortValue:
                jsonWriter.WriteNumber(name, shortValue);
                break;
            case int intValue:
                jsonWriter.WriteNumber(name, intValue);
                break;
            case long longValue:
                jsonWriter.WriteNumber(name, longValue);
                break;
            case float floatValue:
                jsonWriter.WriteNumber(name, floatValue);
                break;
            case double doubleValue:
                jsonWriter.WriteNumber(name, doubleValue);
                break;
            case decimal decimalValue:
                jsonWriter.WriteNumber(name, decimalValue);
                break;
            case Guid guidValue:
                jsonWriter.WriteString(name, guidValue);
                break;
            case DateTime dateTimeValue:
                jsonWriter.WriteString(name, dateTimeValue);
                break;
            case DateTimeOffset dateTimeOffsetValue:
                jsonWriter.WriteString(name, dateTimeOffsetValue);
                break;
            default:
                jsonWriter.WriteString(name, value.ToString());
                break;
        }
    }
}
