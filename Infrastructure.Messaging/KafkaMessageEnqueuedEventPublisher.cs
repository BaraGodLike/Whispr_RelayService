using Application.Abstractions;
using Confluent.Kafka;
using Domain;
using Google.Protobuf;
using Infrastructure.Messaging.Options;
using Microsoft.Extensions.Options;

namespace Infrastructure.Messaging;

public sealed class KafkaMessageEnqueuedEventPublisher(
    IProducer<string, byte[]> producer,
    IOptions<KafkaOptions> options) : IMessageEnqueuedEventPublisher
{
    private readonly KafkaOptions _options = options.Value;

    public async Task PublishAsync(OutboxEvent outboxEvent, CancellationToken cancellationToken)
    {
        var payload = Serialize(outboxEvent);

        await producer.ProduceAsync(
            _options.Topic,
            new Message<string, byte[]>
            {
                Key = outboxEvent.DestMailbox.ToString("D"),
                Value = payload
            },
            cancellationToken);
    }

    private static byte[] Serialize(OutboxEvent outboxEvent)
    {
        using var stream = new MemoryStream();
        using var output = new CodedOutputStream(stream, leaveOpen: true);

        output.WriteTag(1, WireFormat.WireType.LengthDelimited);
        output.WriteString(outboxEvent.MsgId.ToString("D"));

        output.WriteTag(2, WireFormat.WireType.LengthDelimited);
        output.WriteString(outboxEvent.DestMailbox.ToString("D"));

        output.Flush();
        return stream.ToArray();
    }
}
