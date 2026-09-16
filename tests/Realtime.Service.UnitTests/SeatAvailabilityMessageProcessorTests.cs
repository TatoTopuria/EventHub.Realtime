using System.Text.Json;
using BuildingBlocks.Abstractions.Messaging;
using FluentAssertions;
using Moq;
using Realtime.Service.Configuration;
using Realtime.Service.Messaging;
using Realtime.Service.Services;

namespace Realtime.Service.UnitTests;

public sealed class SeatAvailabilityMessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_Should_BroadcastSeatReserved_ForValidReservedMessage()
    {
        var eventId = Guid.NewGuid();
        var broadcaster = new Mock<ISeatAvailabilityBroadcaster>();
        broadcaster
            .Setup(value => value.BroadcastSeatReservedAsync(eventId, "A1", It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var processor = CreateProcessor(broadcaster.Object);
        var payload = JsonSerializer.Serialize(new SeatReservedIntegrationEvent(
            Guid.NewGuid(),
            Guid.NewGuid().ToString("N"),
            eventId,
            "A1",
            DateTime.UtcNow));

        await processor.ProcessAsync("booking.seat.reserved", payload, CancellationToken.None);

        broadcaster.VerifyAll();
    }

    [Fact]
    public async Task ProcessAsync_Should_ThrowPermanentFailure_ForInvalidJson()
    {
        var processor = CreateProcessor(Mock.Of<ISeatAvailabilityBroadcaster>());

        var act = async () => await processor.ProcessAsync("booking.seat.reserved", "{not-json}", CancellationToken.None);

        var exception = await act.Should().ThrowAsync<SeatAvailabilityPermanentFailureException>();
        exception.Which.Reason.Should().Be("InvalidJson");
    }

    [Fact]
    public async Task ProcessAsync_Should_ThrowPermanentFailure_ForUnsupportedRoutingKey()
    {
        var processor = CreateProcessor(Mock.Of<ISeatAvailabilityBroadcaster>());
        var payload = JsonSerializer.Serialize(new SeatReservedIntegrationEvent(
            Guid.NewGuid(),
            Guid.NewGuid().ToString("N"),
            Guid.NewGuid(),
            "A1",
            DateTime.UtcNow));

        var act = async () => await processor.ProcessAsync("booking.seat.updated", payload, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<SeatAvailabilityPermanentFailureException>();
        exception.Which.Reason.Should().Be("UnsupportedRoutingKey");
    }

    [Fact]
    public async Task ProcessAsync_Should_ThrowPermanentFailure_ForMissingSeatNumber()
    {
        var processor = CreateProcessor(Mock.Of<ISeatAvailabilityBroadcaster>());
        var payload = JsonSerializer.Serialize(new SeatReleasedIntegrationEvent(
            Guid.NewGuid(),
            Guid.NewGuid().ToString("N"),
            Guid.NewGuid(),
            " ",
            DateTime.UtcNow));

        var act = async () => await processor.ProcessAsync("booking.seat.released", payload, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<SeatAvailabilityPermanentFailureException>();
        exception.Which.Reason.Should().Be("MissingSeatNumber");
    }

    private static SeatAvailabilityMessageProcessor CreateProcessor(ISeatAvailabilityBroadcaster broadcaster)
    {
        return new SeatAvailabilityMessageProcessor(
            broadcaster,
            new BookingRealtimeMessagingOptions
            {
                SeatReservedRoutingKey = "booking.seat.reserved",
                SeatReleasedRoutingKey = "booking.seat.released"
            });
    }
}