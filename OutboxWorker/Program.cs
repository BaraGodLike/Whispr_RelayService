using Application;
using Infrastructure.Messaging;
using Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OutboxWorker.Logging;
using Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Services.Configure<RelayJsonConsoleFormatterOptions>(builder.Configuration.GetSection("ServiceMetadata"));
builder.Logging.AddConfiguration(builder.Configuration.GetSection("Logging"));
builder.Logging.AddFilter("Startup", LogLevel.Information);
builder.Logging.AddFilter("Application", LogLevel.Information);
builder.Logging.AddFilter("Infrastructure.Storage", LogLevel.Warning);
builder.Logging.AddFilter("Infrastructure.Messaging", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddConsoleFormatter<RelayJsonConsoleFormatter, RelayJsonConsoleFormatterOptions>();
builder.Logging.AddConsole(options => options.FormatterName = RelayJsonConsoleFormatter.FormatterName);

builder.Services
    .AddApplicationServices(builder.Configuration)
    .AddStorageInfrastructure(builder.Configuration)
    .AddMessagingInfrastructure(builder.Configuration)
    .AddWorkerServices();

using var host = builder.Build();

host.Services
    .GetRequiredService<ILoggerFactory>()
    .CreateLogger("Startup")
    .LogInformation("Outbox worker startup completed.");

await host.RunAsync();
