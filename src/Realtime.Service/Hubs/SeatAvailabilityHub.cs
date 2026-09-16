using Microsoft.AspNetCore.SignalR;
using Realtime.Service.Services;

namespace Realtime.Service.Hubs;

public sealed class SeatAvailabilityHub(EventGroupNameResolver eventGroupNameResolver) : Hub
{
    /// <summary>
    /// Joins the caller to the seat availability group for the specified event.
    /// </summary>
    public Task JoinEvent(Guid eventId)
    {
        return Groups.AddToGroupAsync(Context.ConnectionId, eventGroupNameResolver.GetSeatAvailabilityGroupName(eventId));
    }

    /// <summary>
    /// Removes the caller from the seat availability group for the specified event.
    /// </summary>
    public Task LeaveEvent(Guid eventId)
    {
        return Groups.RemoveFromGroupAsync(Context.ConnectionId, eventGroupNameResolver.GetSeatAvailabilityGroupName(eventId));
    }
}