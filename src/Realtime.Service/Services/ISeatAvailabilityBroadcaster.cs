namespace Realtime.Service.Services;

public interface ISeatAvailabilityBroadcaster
{
    Task BroadcastSeatReservedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default);

    Task BroadcastSeatReleasedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default);
}