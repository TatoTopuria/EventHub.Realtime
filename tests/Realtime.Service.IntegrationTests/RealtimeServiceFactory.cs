using DotNet.Testcontainers.Builders;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;

namespace Realtime.Service.IntegrationTests;

public sealed class RealtimeServiceFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    private readonly Action<IServiceCollection>? _configureTestServices;
    private readonly string _queueName = $"realtime.test.{Guid.NewGuid():N}";
    private RabbitMqContainer? _rabbitMqContainer;
    private RedisContainer? _redisContainer;

    public RealtimeServiceFactory()
    {
    }

    internal RealtimeServiceFactory(Action<IServiceCollection> configureTestServices)
    {
        _configureTestServices = configureTestServices;
    }

    public bool IsDockerAvailable { get; private set; } = true;

    public string RabbitMqHostName => _rabbitMqContainer?.Hostname ?? "localhost";

    public int RabbitMqPort => _rabbitMqContainer?.GetMappedPublicPort(5672) ?? 5672;

    public string RedisConnectionString => _redisContainer?.GetConnectionString() ?? "localhost:6379";

    public string QueueName => _queueName;

    public string RetryQueueName => $"{_queueName}.retry";

    public int RetryDelayMilliseconds => 200;

    public int MaxRetryAttempts => 2;

    public string DeadLetterExchangeName => $"{_queueName}.dead-letter";

    public string DeadLetterQueueName => $"{_queueName}.dead-letter";

    public string DeadLetterRoutingKey => DeadLetterQueueName;

    public async Task InitializeAsync()
    {
        try
        {
            _rabbitMqContainer = new RabbitMqBuilder()
                .WithImage("rabbitmq:3-management")
                .WithUsername("guest")
                .WithPassword("guest")
                .Build();

            _redisContainer = new RedisBuilder()
                .WithImage("redis:7")
                .Build();

            await _rabbitMqContainer.StartAsync();
            await _redisContainer.StartAsync();
        }
        catch
        {
            IsDockerAvailable = false;
        }
    }

    public new async Task DisposeAsync()
    {
        if (_rabbitMqContainer is not null)
        {
            await _rabbitMqContainer.StopAsync();
            await _rabbitMqContainer.DisposeAsync();
        }

        if (_redisContainer is not null)
        {
            await _redisContainer.StopAsync();
            await _redisContainer.DisposeAsync();
        }
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");

        builder.ConfigureServices(services => _configureTestServices?.Invoke(services));

        builder.ConfigureAppConfiguration((_, configBuilder) =>
        {
            configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RealtimeMessaging:HostName"] = RabbitMqHostName,
                ["RealtimeMessaging:Port"] = RabbitMqPort.ToString(),
                ["RealtimeMessaging:UserName"] = "guest",
                ["RealtimeMessaging:Password"] = "guest",
                ["RealtimeMessaging:Exchange"] = "eventhub.booking",
                ["RealtimeMessaging:Queue"] = QueueName,
                ["RealtimeMessaging:RetryQueue"] = RetryQueueName,
                ["RealtimeMessaging:RetryDelayMilliseconds"] = RetryDelayMilliseconds.ToString(),
                ["RealtimeMessaging:MaxRetryAttempts"] = MaxRetryAttempts.ToString(),
                ["RealtimeMessaging:DeadLetterExchange"] = DeadLetterExchangeName,
                ["RealtimeMessaging:DeadLetterQueue"] = DeadLetterQueueName,
                ["RealtimeMessaging:DeadLetterRoutingKey"] = DeadLetterRoutingKey,
                ["RealtimeMessaging:SeatReservedRoutingKey"] = "booking.seat.reserved",
                ["RealtimeMessaging:SeatReleasedRoutingKey"] = "booking.seat.released",
                ["RealtimeRedis:ConnectionString"] = RedisConnectionString
            });
        });
    }
}