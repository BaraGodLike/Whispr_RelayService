using Application.Abstractions;
using Confluent.Kafka;
using Infrastructure.Messaging.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Infrastructure.Messaging;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMessagingInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));

        services.AddSingleton<IProducer<string, byte[]>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("Infrastructure.Messaging.KafkaProducer");
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                ClientId = options.ClientId,
                Acks = Acks.All,
                EnableIdempotence = true
            };

            return new ProducerBuilder<string, byte[]>(producerConfig)
                .SetLogHandler((_, message) => LogKafkaMessage(logger, message))
                .Build();
        });

        services.AddSingleton<IAdminClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("Infrastructure.Messaging.KafkaAdmin");
            var config = new AdminClientConfig
            {
                BootstrapServers = options.BootstrapServers,
                ClientId = options.ClientId
            };

            return new AdminClientBuilder(config)
                .SetLogHandler((_, message) => LogKafkaMessage(logger, message))
                .Build();
        });

        services.AddScoped<IMessageEnqueuedEventPublisher, KafkaMessageEnqueuedEventPublisher>();
        services.AddSingleton<KafkaHealthCheck>();

        return services;
    }

    private static void LogKafkaMessage(ILogger logger, LogMessage message)
    {
        var logLevel = IsExpectedTransientKafkaMessage(message)
            ? LogLevel.Warning
            : message.Level switch
        {
            SyslogLevel.Emergency => LogLevel.Critical,
            SyslogLevel.Alert => LogLevel.Critical,
            SyslogLevel.Critical => LogLevel.Critical,
            SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice => LogLevel.Information,
            SyslogLevel.Info => LogLevel.Information,
            SyslogLevel.Debug => LogLevel.Debug,
            _ => LogLevel.Information
        };

        logger.Log(
            logLevel,
            "Kafka client log. Facility: {Facility}, Name: {Name}, Message: {KafkaMessage}.",
            message.Facility,
            message.Name,
            message.Message);
    }

    private static bool IsExpectedTransientKafkaMessage(LogMessage message)
    {
        return message.Message.Contains("Connection refused", StringComparison.OrdinalIgnoreCase)
               || message.Message.Contains("brokers are down", StringComparison.OrdinalIgnoreCase)
               || message.Message.Contains("Coordinator load in progress", StringComparison.OrdinalIgnoreCase)
               || message.Message.Contains("Failed to acquire idempotence PID", StringComparison.OrdinalIgnoreCase);
    }
}
