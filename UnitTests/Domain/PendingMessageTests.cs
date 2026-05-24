using Domain;

namespace UnitTests.Domain;

[TestClass]
public sealed class PendingMessageTests
{
    [TestMethod]
    public void Create_CopiesPayload()
    {
        var payload = new byte[] { 1, 2, 3 };

        var pendingMessage = PendingMessage.Create(Guid.NewGuid(), Guid.NewGuid(), payload);
        payload[0] = 9;

        CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, pendingMessage.Payload);
    }
}
