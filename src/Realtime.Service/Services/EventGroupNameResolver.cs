namespace Realtime.Service.Services;

public sealed class EventGroupNameResolver
{
    public string GetSeatAvailabilityGroupName(Guid eventId)
    {
        if (eventId == Guid.Empty)
        {
            throw new ArgumentException("Event id cannot be empty.", nameof(eventId));
        }

        return $"event-{eventId:D}";
    }
}