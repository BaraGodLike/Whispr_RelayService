namespace Infrastructure.Caching;

internal static class CacheKeys
{
    public static string Message(Guid msgId) => $"msg:{msgId:D}";
}
