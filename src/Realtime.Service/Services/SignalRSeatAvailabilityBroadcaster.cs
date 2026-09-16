using Microsoft.AspNetCore.SignalR;
using Realtime.Service.Hubs;

namespace Realtime.Service.Services;

public sealed class SignalRSeatAvailabilityBroadcaster(
    IHubContext<SeatAvailabilityHub> hubContext,
    EventGroupNameResolver eventGroupNameResolver) : ISeatAvailabilityBroadcaster
{
    public Task BroadcastSeatReservedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default)
    {
        return hubContext.Clients
            .Group(eventGroupNameResolver.GetSeatAvailabilityGroupName(eventId))
            .SendAsync("SeatReserved", eventId, seatNumber, cancellationToken);
    }

    public Task BroadcastSeatReleasedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default)
    {
        return hubContext.Clients
            .Group(eventGroupNameResolver.GetSeatAvailabilityGroupName(eventId))
            .SendAsync("SeatReleased", eventId, seatNumber, cancellationToken);
    }
}