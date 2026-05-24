using Application.Contracts;

namespace Application.Abstractions;

public interface IExpiredMessageCleanupService
{
    Task<ExpiredMessageCleanupResult> CleanupExpiredMessagesAsync(CancellationToken cancellationToken);
}
