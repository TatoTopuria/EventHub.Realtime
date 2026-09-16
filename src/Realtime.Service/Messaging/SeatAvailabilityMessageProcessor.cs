using System.Text.Json;
using BuildingBlocks.Abstractions.Messaging;
using Realtime.Service.Configuration;
using Realtime.Service.Services;

namespace Realtime.Service.Messaging;

internal sealed class SeatAvailabilityMessageProcessor(
    ISeatAvailabilityBroadcaster broadcaster,
    BookingRealtimeMessagingOptions options)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public Task ProcessAsync(string routingKey, string payload, CancellationToken cancellationToken)
    {
        return routingKey switch
        {
            var key when key == options.SeatReservedRoutingKey => HandleSeatReservedAsync(payload, cancellationToken),
            var key when key == options.SeatReleasedRoutingKey => HandleSeatReleasedAsync(payload, cancellationToken),
            _ => throw new SeatAvailabilityPermanentFailureException(
                "UnsupportedRoutingKey",
                $"Unsupported routing key '{routingKey}'.")
        };
    }

    private async Task HandleSeatReservedAsync(string payload, CancellationToken cancellationToken)
    {
        var integrationEvent = Deserialize<SeatReservedIntegrationEvent>(payload, nameof(SeatReservedIntegrationEvent));
        EnsureSeatEventIsValid(integrationEvent.EventId, integrationEvent.SeatNumber);
        await broadcaster.BroadcastSeatReservedAsync(integrationEvent.EventId, integrationEvent.SeatNumber, cancellationToken);
    }

    private async Task HandleSeatReleasedAsync(string payload, CancellationToken cancellationToken)
    {
        var integrationEvent = Deserialize<SeatReleasedIntegrationEvent>(payload, nameof(SeatReleasedIntegrationEvent));
        EnsureSeatEventIsValid(integrationEvent.EventId, integrationEvent.SeatNumber);
        await broadcaster.BroadcastSeatReleasedAsync(integrationEvent.EventId, integrationEvent.SeatNumber, cancellationToken);
    }

    private static T Deserialize<T>(string payload, string messageType) where T : class
    {
        try
        {
            var result = JsonSerializer.Deserialize<T>(payload, SerializerOptions);
            if (result is null)
            {
                throw new SeatAvailabilityPermanentFailureException(
                    "NullPayload",
                    $"{messageType} payload was null.");
            }

            return result;
        }
        catch (JsonException exception)
        {
            throw new SeatAvailabilityPermanentFailureException(
                "InvalidJson",
                $"{messageType} payload was invalid JSON.",
                exception);
        }
    }

    private static void EnsureSeatEventIsValid(Guid eventId, string? seatNumber)
    {
        if (eventId == Guid.Empty)
        {
            throw new SeatAvailabilityPermanentFailureException(
                "MissingEventId",
                "Seat availability event was missing EventId.");
        }

        if (string.IsNullOrWhiteSpace(seatNumber))
        {
            throw new SeatAvailabilityPermanentFailureException(
                "MissingSeatNumber",
                "Seat availability event was missing SeatNumber.");
        }
    }
}

internal sealed class SeatAvailabilityPermanentFailureException : Exception
{
    public SeatAvailabilityPermanentFailureException(string reason, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Reason = reason;
    }

    public string Reason { get; }
}