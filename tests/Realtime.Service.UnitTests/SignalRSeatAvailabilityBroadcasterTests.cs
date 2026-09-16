using Microsoft.AspNetCore.SignalR;
using Moq;
using Realtime.Service.Hubs;
using Realtime.Service.Services;

namespace Realtime.Service.UnitTests;

public sealed class SignalRSeatAvailabilityBroadcasterTests
{
    [Fact]
    public async Task BroadcastSeatReservedAsync_Should_Target_EventSpecificGroup()
    {
        var eventId = Guid.NewGuid();
        var expectedGroup = $"event-{eventId:D}";
        var clientProxy = new Mock<IClientProxy>();
        var hubClients = new Mock<IHubClients>();
        var hubContext = new Mock<IHubContext<SeatAvailabilityHub>>();

        hubClients.Setup(clients => clients.Group(expectedGroup)).Returns(clientProxy.Object);
        hubContext.SetupGet(context => context.Clients).Returns(hubClients.Object);

        var broadcaster = new SignalRSeatAvailabilityBroadcaster(hubContext.Object, new EventGroupNameResolver());

        await broadcaster.BroadcastSeatReservedAsync(eventId, "A1");

        hubClients.Verify(clients => clients.Group(expectedGroup), Times.Once);
        clientProxy.Verify(proxy => proxy.SendCoreAsync(
            "SeatReserved",
            It.Is<object?[]>(values => (Guid)values[0]! == eventId && (string)values[1]! == "A1"),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}