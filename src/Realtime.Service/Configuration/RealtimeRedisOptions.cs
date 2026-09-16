namespace Realtime.Service.Configuration;

public sealed class RealtimeRedisOptions
{
    public const string SectionName = "RealtimeRedis";

    public string ConnectionString { get; init; } = "localhost:6379";

    public int ProcessedMessageTtlHours { get; init; } = 24;

    public int ProcessingLockMinutes { get; init; } = 2;
}