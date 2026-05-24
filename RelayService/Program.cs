using Application;
using Infrastructure.Caching;
using Infrastructure.Messaging;
using Infrastructure.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RelayService.Logging;
using RelayService.Services;
using Worker;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Services.Configure<RelayJsonConsoleFormatterOptions>(builder.Configuration.GetSection("ServiceMetadata"));
builder.Logging.AddConfiguration(builder.Configuration.GetSection("Logging"));
builder.Logging.AddFilter("Startup", LogLevel.Information);
builder.Logging.AddFilter("Application", LogLevel.Information);
builder.Logging.AddFilter("Infrastructure.Storage", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddConsoleFormatter<RelayJsonConsoleFormatter, RelayJsonConsoleFormatterOptions>();
builder.Logging.AddConsole(options => options.FormatterName = RelayJsonConsoleFormatter.FormatterName);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8080, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
});

builder.Services.AddGrpc();
builder.Services.AddGrpcHealthChecks();

builder.Services
    .AddApplicationServices(builder.Configuration)
    .AddStorageInfrastructure(builder.Configuration)
    .AddCachingInfrastructure(builder.Configuration)
    .AddMessagingInfrastructure(builder.Configuration)
    .AddWorkerServices();

builder.Services
    .AddHealthChecks()
    .AddCheck<PostgresHealthCheck>("postgres")
    .AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

app.Services
    .GetRequiredService<ILoggerFactory>()
    .CreateLogger("Startup")
    .LogInformation("Relay service startup completed.");

app.MapGrpcService<RelayGrpcService>();
app.MapGrpcHealthChecksService();

app.Run();
