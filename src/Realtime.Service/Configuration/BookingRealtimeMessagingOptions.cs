namespace Realtime.Service.Configuration;

public sealed class BookingRealtimeMessagingOptions
{
    public const string SectionName = "RealtimeMessaging";

    public string HostName { get; init; } = "localhost";

    public int Port { get; init; } = 5672;

    public string UserName { get; init; } = "guest";

    public string Password { get; init; } = "guest";

    public string Exchange { get; init; } = "eventhub.booking";

    public string Queue { get; init; } = "realtime.seat-availability";

    public string RetryQueue { get; init; } = string.Empty;

    public int RetryDelayMilliseconds { get; init; } = 1000;

    public int MaxRetryAttempts { get; init; } = 3;

    public string DeadLetterExchange { get; init; } = string.Empty;

    public string DeadLetterQueue { get; init; } = string.Empty;

    public string DeadLetterRoutingKey { get; init; } = string.Empty;

    public string SeatReservedRoutingKey { get; init; } = "booking.seat.reserved";

    public string SeatReleasedRoutingKey { get; init; } = "booking.seat.released";
}