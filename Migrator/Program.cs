using FluentMigrator.Runner;
using Infrastructure.Storage.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
var connectionString = builder.Configuration["Postgres:ConnectionString"] ?? string.Empty;

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole();

builder.Services.Configure<PostgresOptions>(builder.Configuration.GetSection("Postgres"));
builder.Services
    .AddFluentMigratorCore()
    .ConfigureRunner(runner => runner
        .AddPostgres()
        .WithGlobalConnectionString(connectionString)
        .ScanIn(typeof(Program).Assembly).For.Migrations());

using var host = builder.Build();
using var scope = host.Services.CreateScope();

var runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>();
runner.MigrateUp();
