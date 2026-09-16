using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using BuildingBlocks.Abstractions.Messaging;
using FluentAssertions;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Realtime.Service.Services;

namespace Realtime.Service.IntegrationTests;

public sealed class SeatAvailabilityConsumerIntegrationTests(RealtimeServiceFactory factory) : IClassFixture<RealtimeServiceFactory>
{
    [Fact]
    public async Task PublishSeatReservedIntegrationEvent_Should_Reach_ConnectedSignalRClient_WithinTwoSeconds()
    {
        DockerRequirement.SkipIfUnavailable(factory.IsDockerAvailable);

        var eventId = Guid.NewGuid();
        var receivedSeats = new ConcurrentQueue<string>();
        var completionSource = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var connection = new HubConnectionBuilder()
            .WithUrl(new Uri(factory.Server.BaseAddress!, "/hubs/seats"), options =>
            {
                options.HttpMessageHandlerFactory = _ => factory.Server.CreateHandler();
                options.Transports = HttpTransportType.LongPolling;
            })
            .Build();

        connection.On<Guid, string>("SeatReserved", (receivedEventId, seatNumber) =>
        {
            if (receivedEventId != eventId)
            {
                return;
            }

            receivedSeats.Enqueue(seatNumber);
            completionSource.TrySetResult(seatNumber);
        });

        await connection.StartAsync();
        await connection.InvokeAsync("JoinEvent", eventId);

        await PublishSeatReservedAsync(factory, eventId, "A1");

        var completedTask = await Task.WhenAny(completionSource.Task, Task.Delay(TimeSpan.FromSeconds(2)));

        await connection.DisposeAsync();

        completedTask.Should().Be(completionSource.Task);
        receivedSeats.Should().ContainSingle().Which.Should().Be("A1");
    }

    [Fact]
    public async Task PublishSeatReservedIntegrationEvent_WithDuplicateMessageId_Should_Reach_ConnectedSignalRClient_OnlyOnce()
    {
        DockerRequirement.SkipIfUnavailable(factory.IsDockerAvailable);

        var eventId = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid().ToString("N");
        var receivedSeats = new ConcurrentQueue<string>();
        var completionSource = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var connection = new HubConnectionBuilder()
            .WithUrl(new Uri(factory.Server.BaseAddress!, "/hubs/seats"), options =>
            {
                options.HttpMessageHandlerFactory = _ => factory.Server.CreateHandler();
                options.Transports = HttpTransportType.LongPolling;
            })
            .Build();

        connection.On<Guid, string>("SeatReserved", (receivedEventId, seatNumber) =>
        {
            if (receivedEventId != eventId)
            {
                return;
            }

            receivedSeats.Enqueue(seatNumber);
            completionSource.TrySetResult(seatNumber);
        });

        await connection.StartAsync();
        await connection.InvokeAsync("JoinEvent", eventId);

        await PublishSeatReservedAsync(factory, eventId, "A1", messageId, correlationId);
        await PublishSeatReservedAsync(factory, eventId, "A1", messageId, correlationId);

        var completedTask = await Task.WhenAny(completionSource.Task, Task.Delay(TimeSpan.FromSeconds(2)));
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        await connection.DisposeAsync();

        completedTask.Should().Be(completionSource.Task);
        receivedSeats.Should().ContainSingle().Which.Should().Be("A1");
    }

    [Fact]
    public async Task InvalidJsonPayload_Should_Be_DeadLettered_WithoutInfiniteRetryLoop()
    {
        DockerRequirement.SkipIfUnavailable(factory.IsDockerAvailable);

        using var client = factory.CreateClient();
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid().ToString("N");

        PublishMessage(factory, "booking.seat.reserved", "{not-json}", messageId, correlationId);

        var deadLetter = await WaitForQueueMessageAsync(factory, factory.DeadLetterQueueName, TimeSpan.FromSeconds(5));

        deadLetter.Should().NotBeNull();
        DecodeHeaderValue(deadLetter!.BasicProperties.Headers, "x-dead-letter-reason").Should().Be("InvalidJson");
        DecodeHeaderValue(deadLetter.BasicProperties.Headers, "x-failure-classification").Should().Be("permanent");
        DecodeHeaderValue(deadLetter.BasicProperties.Headers, "x-original-routing-key").Should().Be("booking.seat.reserved");
        deadLetter.BasicProperties.MessageId.Should().Be(messageId.ToString("D"));
        deadLetter.BasicProperties.CorrelationId.Should().Be(correlationId);

        await Task.Delay(TimeSpan.FromMilliseconds(factory.RetryDelayMilliseconds * (factory.MaxRetryAttempts + 1) + 250));

        GetReadyMessageCount(factory, factory.QueueName).Should().Be(0);
        GetReadyMessageCount(factory, factory.RetryQueueName).Should().Be(0);
    }

    [Fact]
    public async Task TransientBroadcasterFailure_Should_Retry_And_Recover_WithinConfiguredBudget()
    {
        var broadcaster = new FlakyBroadcaster(failuresBeforeSuccess: 1);
        var localFactory = new RealtimeServiceFactory(services =>
        {
            services.RemoveAll<ISeatAvailabilityBroadcaster>();
            services.AddSingleton<ISeatAvailabilityBroadcaster>(broadcaster);
        });

        await localFactory.InitializeAsync();

        try
        {
            DockerRequirement.SkipIfUnavailable(localFactory.IsDockerAvailable);

            using var client = localFactory.CreateClient();
            var eventId = Guid.NewGuid();
            var messageId = Guid.NewGuid();

            await PublishSeatReservedAsync(localFactory, eventId, "A1", messageId, Guid.NewGuid().ToString("N"));

            var completedTask = await Task.WhenAny(broadcaster.Success.Task, Task.Delay(TimeSpan.FromSeconds(5)));

            completedTask.Should().Be(broadcaster.Success.Task);
            broadcaster.CallCount.Should().Be(2);
            GetReadyMessageCount(localFactory, localFactory.RetryQueueName).Should().Be(0);
            GetReadyMessageCount(localFactory, localFactory.DeadLetterQueueName).Should().Be(0);
        }
        finally
        {
            localFactory.Dispose();
            await localFactory.DisposeAsync();
        }
    }

    private static Task PublishSeatReservedAsync(
        RealtimeServiceFactory factory,
        Guid eventId,
        string seatNumber,
        Guid? messageId = null,
        string? correlationId = null)
    {
        var integrationEvent = new SeatReservedIntegrationEvent(
            messageId ?? Guid.NewGuid(),
            correlationId ?? Guid.NewGuid().ToString("N"),
            eventId,
            seatNumber,
            DateTime.UtcNow);

        PublishMessage(
            factory,
            "booking.seat.reserved",
            JsonSerializer.Serialize(integrationEvent),
            integrationEvent.MessageId,
            integrationEvent.CorrelationId);

        return Task.CompletedTask;
    }

    private static void PublishMessage(
        RealtimeServiceFactory factory,
        string routingKey,
        string payload,
        Guid messageId,
        string correlationId)
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = factory.RabbitMqHostName,
            Port = factory.RabbitMqPort,
            UserName = "guest",
            Password = "guest"
        };

        using var connection = connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare("eventhub.booking", ExchangeType.Topic, durable: true, autoDelete: false);

        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.MessageId = messageId.ToString("D");
        properties.CorrelationId = correlationId;

        channel.BasicPublish(
            exchange: "eventhub.booking",
            routingKey: routingKey,
            basicProperties: properties,
            body: Encoding.UTF8.GetBytes(payload));
    }

    private static async Task<BasicGetResult?> WaitForQueueMessageAsync(
        RealtimeServiceFactory factory,
        string queueName,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow.Add(timeout);
        var connectionFactory = new ConnectionFactory
        {
            HostName = factory.RabbitMqHostName,
            Port = factory.RabbitMqPort,
            UserName = "guest",
            Password = "guest"
        };

        using var connection = connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();

        while (DateTime.UtcNow < deadline)
        {
            var result = channel.BasicGet(queueName, autoAck: true);
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(100);
        }

        return null;
    }

    private static uint GetReadyMessageCount(RealtimeServiceFactory factory, string queueName)
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = factory.RabbitMqHostName,
            Port = factory.RabbitMqPort,
            UserName = "guest",
            Password = "guest"
        };

        using var connection = connectionFactory.CreateConnection();
        using var channel = connection.CreateModel();

        return channel.QueueDeclarePassive(queueName).MessageCount;
    }

    private static string? DecodeHeaderValue(IDictionary<string, object?>? headers, string key)
    {
        if (headers is null || !headers.TryGetValue(key, out var rawValue))
        {
            return null;
        }

        return rawValue switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.ToArray()),
            _ => rawValue?.ToString()
        };
    }

    private sealed class FlakyBroadcaster(int failuresBeforeSuccess) : ISeatAvailabilityBroadcaster
    {
        private int _remainingFailures = failuresBeforeSuccess;
        private int _callCount;

        public int CallCount => _callCount;

        public TaskCompletionSource<bool> Success { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task BroadcastSeatReservedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _callCount);

            if (Interlocked.Decrement(ref _remainingFailures) >= 0)
            {
                throw new InvalidOperationException("Simulated broadcaster failure.");
            }

            Success.TrySetResult(true);
            return Task.CompletedTask;
        }

        public Task BroadcastSeatReleasedAsync(Guid eventId, string seatNumber, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}