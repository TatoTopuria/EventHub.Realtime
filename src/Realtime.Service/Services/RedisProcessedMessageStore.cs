using Microsoft.Extensions.Options;
using Realtime.Service.Configuration;
using StackExchange.Redis;

namespace Realtime.Service.Services;

public sealed class RedisProcessedMessageStore(
    IConnectionMultiplexer connectionMultiplexer,
    IOptions<RealtimeRedisOptions> options) : IProcessedMessageStore
{
    private readonly IDatabase _database = connectionMultiplexer.GetDatabase();
    private readonly RealtimeRedisOptions _options = options.Value;

    public Task<bool> TryBeginProcessingAsync(string messageId, CancellationToken cancellationToken = default)
    {
        return _database.StringSetAsync(
            GetKey(messageId),
            "processing",
            TimeSpan.FromMinutes(_options.ProcessingLockMinutes),
            when: When.NotExists);
    }

    public Task MarkProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        return _database.StringSetAsync(
            GetKey(messageId),
            "processed",
            TimeSpan.FromHours(_options.ProcessedMessageTtlHours));
    }

    public Task ReleaseAsync(string messageId, CancellationToken cancellationToken = default)
    {
        return _database.KeyDeleteAsync(GetKey(messageId));
    }

    private static string GetKey(string messageId) => $"realtime:inbox:{messageId}";
}