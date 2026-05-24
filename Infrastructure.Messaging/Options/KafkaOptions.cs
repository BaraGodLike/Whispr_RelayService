namespace Infrastructure.Messaging.Options;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;

    public string Topic { get; set; } = Domain.RelayConstraints.MessageEnqueuedEventType;

    public string ClientId { get; set; } = "relay-service";
}
