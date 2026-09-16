namespace Realtime.Service.Services;

public interface IProcessedMessageStore
{
    Task<bool> TryBeginProcessingAsync(string messageId, CancellationToken cancellationToken = default);

    Task MarkProcessedAsync(string messageId, CancellationToken cancellationToken = default);

    Task ReleaseAsync(string messageId, CancellationToken cancellationToken = default);
}