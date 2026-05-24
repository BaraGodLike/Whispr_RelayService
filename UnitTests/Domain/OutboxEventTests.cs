using Domain;

namespace UnitTests.Domain;

[TestClass]
public sealed class OutboxEventTests
{
    [TestMethod]
    public void CreateMessageEnqueued_UsesExpectedEventType()
    {
        var outboxEvent = OutboxEvent.CreateMessageEnqueued(Guid.NewGuid(), Guid.NewGuid());

        Assert.AreEqual(RelayConstraints.MessageEnqueuedEventType, outboxEvent.EventType);
        Assert.IsFalse(outboxEvent.Published);
        Assert.AreEqual(DateTimeOffset.MinValue, outboxEvent.CreatedAt);
    }
}
