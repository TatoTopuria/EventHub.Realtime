using Realtime.Service.Configuration;
using Realtime.Service.Hubs;
using Realtime.Service.Messaging;
using Realtime.Service.Middleware;
using Realtime.Service.Services;
using StackExchange.Redis;
using BuildingBlocks.Observability;
using BuildingBlocks.Time;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

builder.AddEventHubSerilog();
builder.AddEventHubTracing();
builder.Services.AddCorrelationContext();

builder.Services.Configure<BookingRealtimeMessagingOptions>(builder.Configuration.GetSection(BookingRealtimeMessagingOptions.SectionName));
builder.Services.Configure<RealtimeRedisOptions>(builder.Configuration.GetSection(RealtimeRedisOptions.SectionName));

builder.Services.AddSignalR();
builder.Services.AddEventHubClock();
builder.Services.AddSingleton<EventGroupNameResolver>();
builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    var options = serviceProvider.GetRequiredService<IConfiguration>()
        .GetSection(RealtimeRedisOptions.SectionName)
        .Get<RealtimeRedisOptions>() ?? new RealtimeRedisOptions();

    return ConnectionMultiplexer.Connect(options.ConnectionString);
});
builder.Services.AddSingleton<IProcessedMessageStore, RedisProcessedMessageStore>();
builder.Services.AddSingleton<ISeatAvailabilityBroadcaster, SignalRSeatAvailabilityBroadcaster>();
builder.Services.AddHostedService<BookingSeatAvailabilityConsumer>();

var realtimeMessagingSection = builder.Configuration.GetSection(BookingRealtimeMessagingOptions.SectionName);
var realtimeRmqHost = realtimeMessagingSection["HostName"] ?? "localhost";
var realtimeRmqPort = realtimeMessagingSection["Port"] ?? "5672";
var realtimeRmqUser = realtimeMessagingSection["UserName"] ?? "guest";
var realtimeRmqPass = realtimeMessagingSection["Password"] ?? "guest";
var realtimeRedisConn = builder.Configuration[$"{RealtimeRedisOptions.SectionName}:ConnectionString"] ?? "localhost:6379";

builder.Services.AddHealthChecks()
    .AddRedis(realtimeRedisConn, tags: new[] { "ready" })
    .AddRabbitMQ(new Uri($"amqp://{realtimeRmqUser}:{realtimeRmqPass}@{realtimeRmqHost}:{realtimeRmqPort}"),
        name: "rabbitmq", tags: new[] { "ready" });

var app = builder.Build();

app.UseCorrelationContext();
app.UseMiddleware<GlobalExceptionMiddleware>();
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapHub<SeatAvailabilityHub>("/hubs/seats");

app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
});
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")
});

app.Run();

public partial class Program;