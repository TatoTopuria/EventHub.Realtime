using FluentAssertions;
using Realtime.Service.Services;

namespace Realtime.Service.UnitTests;

public sealed class EventGroupNameResolverTests
{
    [Fact]
    public void GetSeatAvailabilityGroupName_Should_Return_EventPrefixedGroupName()
    {
        var eventId = Guid.NewGuid();
        var resolver = new EventGroupNameResolver();

        var groupName = resolver.GetSeatAvailabilityGroupName(eventId);

        groupName.Should().Be($"event-{eventId:D}");
    }
}