using System.Globalization;
using System.Text;
using BuildingBlocks.Abstractions.Messaging;
using BuildingBlocks.Abstractions.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Realtime.Service.Configuration;
using Realtime.Service.Services;

namespace Realtime.Service.Messaging;

public sealed class BookingSeatAvailabilityConsumer(
    IOptions<BookingRealtimeMessagingOptions> options,
    IProcessedMessageStore processedMessageStore,
    ISeatAvailabilityBroadcaster broadcaster,
    ILogger<BookingSeatAvailabilityConsumer> logger,
    ICorrelationContextAccessor correlationContextAccessor) : BackgroundService
{
    private const string RetryCountHeader = "x-retry-count";
    private const string OriginalRoutingKeyHeader = "x-original-routing-key";
    private const string DeadLetterReasonHeader = "x-dead-letter-reason";
    private const string FailureClassificationHeader = "x-failure-classification";
    private readonly BookingRealtimeMessagingOptions _options = options.Value;
    private readonly SeatAvailabilityMessageProcessor _messageProcessor = new(broadcaster, options.Value);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConsumerLoopAsync(stoppingToken);
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "RealtimeSeatAvailabilityConsumerRetry delaySeconds={DelaySeconds}", 5);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task RunConsumerLoopAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            DispatchConsumersAsync = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        DeclareTopology(channel);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (_, eventArguments) =>
        {
            var body = eventArguments.Body.ToArray();
            var payload = Encoding.UTF8.GetString(body);
            var messageId = ResolveMessageId(eventArguments);
            var correlationId = ResolveCorrelationId(eventArguments);
            var routingKey = ResolveBusinessRoutingKey(eventArguments);
            var retryCount = ResolveRetryCount(eventArguments);
            correlationContextAccessor.CorrelationId = correlationId;

            try
            {
                if (!await processedMessageStore.TryBeginProcessingAsync(messageId, stoppingToken))
                {
                    channel.BasicAck(eventArguments.DeliveryTag, multiple: false);
                    return;
                }

                await _messageProcessor.ProcessAsync(routingKey, payload, stoppingToken);

                await processedMessageStore.MarkProcessedAsync(messageId, stoppingToken);
                channel.BasicAck(eventArguments.DeliveryTag, multiple: false);
            }
            catch (SeatAvailabilityPermanentFailureException exception)
            {
                await HandlePermanentFailureAsync(
                    channel,
                    eventArguments,
                    body,
                    messageId,
                    correlationId,
                    routingKey,
                    retryCount,
                    exception,
                    stoppingToken);
            }
            catch (Exception exception)
            {
                await HandleTransientFailureAsync(
                    channel,
                    eventArguments,
                    body,
                    messageId,
                    correlationId,
                    routingKey,
                    retryCount,
                    exception,
                    stoppingToken);
            }
            finally
            {
                correlationContextAccessor.CorrelationId = null;
            }
        };

        channel.BasicConsume(_options.Queue, autoAck: false, consumer: consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private void DeclareTopology(IModel channel)
    {
        channel.ExchangeDeclare(_options.Exchange, ExchangeType.Topic, durable: true, autoDelete: false);
        channel.QueueDeclare(_options.Queue, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(_options.Queue, _options.Exchange, _options.SeatReservedRoutingKey);
        channel.QueueBind(_options.Queue, _options.Exchange, _options.SeatReleasedRoutingKey);

        channel.QueueDeclare(
            ResolveRetryQueueName(),
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-message-ttl"] = ResolveRetryDelayMilliseconds(),
                ["x-dead-letter-exchange"] = string.Empty,
                ["x-dead-letter-routing-key"] = _options.Queue
            });

        channel.ExchangeDeclare(ResolveDeadLetterExchangeName(), ExchangeType.Direct, durable: true, autoDelete: false);
        channel.QueueDeclare(ResolveDeadLetterQueueName(), durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(ResolveDeadLetterQueueName(), ResolveDeadLetterExchangeName(), ResolveDeadLetterRoutingKey());
    }

    private async Task HandlePermanentFailureAsync(
        IModel channel,
        BasicDeliverEventArgs eventArguments,
        byte[] body,
        string messageId,
        string correlationId,
        string routingKey,
        int retryCount,
        SeatAvailabilityPermanentFailureException exception,
        CancellationToken cancellationToken)
    {
        try
        {
            PublishDeadLetter(
                channel,
                eventArguments,
                body,
                messageId,
                correlationId,
                routingKey,
                retryCount,
                exception.Reason,
                failureClassification: "permanent");

            await processedMessageStore.MarkProcessedAsync(messageId, cancellationToken);

            logger.LogWarning(
                exception,
                "RealtimeSeatAvailabilityConsumerDeadLettered classification={Classification} reason={Reason} routingKey={RoutingKey} messageId={MessageId} correlationId={CorrelationId} retryCount={RetryCount} payloadLength={PayloadLength}",
                "permanent",
                exception.Reason,
                routingKey,
                messageId,
                correlationId,
                retryCount,
                body.Length);

            channel.BasicAck(eventArguments.DeliveryTag, multiple: false);
        }
        catch (Exception handlingException)
        {
            await processedMessageStore.ReleaseAsync(messageId, cancellationToken);
            logger.LogError(
                handlingException,
                "RealtimeSeatAvailabilityConsumerFailurePathFailed classification={Classification} reason={Reason} routingKey={RoutingKey} messageId={MessageId}",
                "permanent",
                exception.Reason,
                routingKey,
                messageId);
            channel.BasicNack(eventArguments.DeliveryTag, multiple: false, requeue: true);
        }
    }

    private async Task HandleTransientFailureAsync(
        IModel channel,
        BasicDeliverEventArgs eventArguments,
        byte[] body,
        string messageId,
        string correlationId,
        string routingKey,
        int retryCount,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            if (retryCount >= ResolveMaxRetryAttempts())
            {
                PublishDeadLetter(
                    channel,
                    eventArguments,
                    body,
                    messageId,
                    correlationId,
                    routingKey,
                    retryCount,
                    reason: "RetryLimitExceeded",
                    failureClassification: "transient");

                await processedMessageStore.MarkProcessedAsync(messageId, cancellationToken);

                logger.LogError(
                    exception,
                    "RealtimeSeatAvailabilityConsumerDeadLettered classification={Classification} reason={Reason} routingKey={RoutingKey} messageId={MessageId} correlationId={CorrelationId} retryCount={RetryCount} payloadLength={PayloadLength}",
                    "transient",
                    "RetryLimitExceeded",
                    routingKey,
                    messageId,
                    correlationId,
                    retryCount,
                    body.Length);
            }
            else
            {
                PublishRetry(channel, eventArguments, body, messageId, correlationId, routingKey, retryCount + 1);
                await processedMessageStore.ReleaseAsync(messageId, cancellationToken);

                logger.LogWarning(
                    exception,
                    "RealtimeSeatAvailabilityConsumerRetryScheduled routingKey={RoutingKey} messageId={MessageId} correlationId={CorrelationId} retryAttempt={RetryAttempt} maxRetryAttempts={MaxRetryAttempts} payloadLength={PayloadLength}",
                    routingKey,
                    messageId,
                    correlationId,
                    retryCount + 1,
                    ResolveMaxRetryAttempts(),
                    body.Length);
            }

            channel.BasicAck(eventArguments.DeliveryTag, multiple: false);
        }
        catch (Exception handlingException)
        {
            await processedMessageStore.ReleaseAsync(messageId, cancellationToken);
            logger.LogError(
                handlingException,
                "RealtimeSeatAvailabilityConsumerFailurePathFailed classification={Classification} routingKey={RoutingKey} messageId={MessageId} retryCount={RetryCount}",
                "transient",
                routingKey,
                messageId,
                retryCount);
            channel.BasicNack(eventArguments.DeliveryTag, multiple: false, requeue: true);
        }
    }

    private void PublishRetry(
        IModel channel,
        BasicDeliverEventArgs eventArguments,
        byte[] body,
        string messageId,
        string correlationId,
        string routingKey,
        int retryCount)
    {
        var properties = CreateForwardingProperties(channel, eventArguments.BasicProperties, messageId, correlationId);
        properties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        properties.Headers[RetryCountHeader] = EncodeHeaderValue(retryCount.ToString(CultureInfo.InvariantCulture));
        properties.Headers[OriginalRoutingKeyHeader] = EncodeHeaderValue(routingKey);

        channel.BasicPublish(
            exchange: string.Empty,
            routingKey: ResolveRetryQueueName(),
            basicProperties: properties,
            body: body);
    }

    private void PublishDeadLetter(
        IModel channel,
        BasicDeliverEventArgs eventArguments,
        byte[] body,
        string messageId,
        string correlationId,
        string routingKey,
        int retryCount,
        string reason,
        string failureClassification)
    {
        var properties = CreateForwardingProperties(channel, eventArguments.BasicProperties, messageId, correlationId);
        properties.Headers ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        properties.Headers[RetryCountHeader] = EncodeHeaderValue(retryCount.ToString(CultureInfo.InvariantCulture));
        properties.Headers[OriginalRoutingKeyHeader] = EncodeHeaderValue(routingKey);
        properties.Headers[DeadLetterReasonHeader] = EncodeHeaderValue(reason);
        properties.Headers[FailureClassificationHeader] = EncodeHeaderValue(failureClassification);

        channel.BasicPublish(
            exchange: ResolveDeadLetterExchangeName(),
            routingKey: ResolveDeadLetterRoutingKey(),
            basicProperties: properties,
            body: body);
    }

    private static IBasicProperties CreateForwardingProperties(
        IModel channel,
        IBasicProperties originalProperties,
        string messageId,
        string correlationId)
    {
        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.ContentType = originalProperties.ContentType;
        properties.ContentEncoding = originalProperties.ContentEncoding;
        properties.Type = originalProperties.Type;
        properties.MessageId = messageId;
        properties.CorrelationId = correlationId;
        properties.Headers = CloneHeaders(originalProperties.Headers);
        return properties;
    }

    private static Dictionary<string, object?> CloneHeaders(IDictionary<string, object?>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        }

        var clonedHeaders = new Dictionary<string, object?>(headers.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in headers)
        {
            clonedHeaders[key] = value switch
            {
                byte[] bytes => bytes.ToArray(),
                ReadOnlyMemory<byte> memory => memory.ToArray(),
                _ => value
            };
        }

        return clonedHeaders;
    }

    private static byte[] EncodeHeaderValue(string value) => Encoding.UTF8.GetBytes(value);

    private static string ResolveMessageId(BasicDeliverEventArgs eventArguments)
    {
        if (!string.IsNullOrWhiteSpace(eventArguments.BasicProperties.MessageId))
        {
            return eventArguments.BasicProperties.MessageId;
        }

        return Guid.NewGuid().ToString("N");
    }

    private static string ResolveBusinessRoutingKey(BasicDeliverEventArgs eventArguments)
    {
        if (eventArguments.BasicProperties.Headers is not null)
        {
            foreach (var (key, rawValue) in eventArguments.BasicProperties.Headers)
            {
                if (!string.Equals(key, OriginalRoutingKeyHeader, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var parsedValue = DecodeHeaderValue(rawValue);
                if (!string.IsNullOrWhiteSpace(parsedValue))
                {
                    return parsedValue;
                }
            }
        }

        return eventArguments.RoutingKey;
    }

    private static int ResolveRetryCount(BasicDeliverEventArgs eventArguments)
    {
        if (eventArguments.BasicProperties.Headers is null)
        {
            return 0;
        }

        foreach (var (key, rawValue) in eventArguments.BasicProperties.Headers)
        {
            if (!string.Equals(key, RetryCountHeader, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return rawValue switch
            {
                byte value => value,
                sbyte value => value,
                short value => value,
                ushort value => value,
                int value => value,
                uint value => checked((int)value),
                long value => checked((int)value),
                ulong value => checked((int)value),
                _ when int.TryParse(DecodeHeaderValue(rawValue), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
                _ => 0
            };
        }

        return 0;
    }

    private static string ResolveCorrelationId(BasicDeliverEventArgs eventArguments)
    {
        if (!string.IsNullOrWhiteSpace(eventArguments.BasicProperties.CorrelationId))
        {
            return eventArguments.BasicProperties.CorrelationId;
        }

        if (eventArguments.BasicProperties.Headers is null)
        {
            return Guid.NewGuid().ToString("N");
        }

        foreach (var (key, rawValue) in eventArguments.BasicProperties.Headers)
        {
            if (!string.Equals(key, "x-correlation-id", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(key, "correlation-id", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var parsedValue = DecodeHeaderValue(rawValue);
            if (!string.IsNullOrWhiteSpace(parsedValue))
            {
                return parsedValue;
            }
        }

        return Guid.NewGuid().ToString("N");
    }

    private static string? DecodeHeaderValue(object? rawValue)
    {
        return rawValue switch
        {
            null => null,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.ToArray()),
            _ => rawValue.ToString()
        };
    }

    private string ResolveRetryQueueName()
    {
        return string.IsNullOrWhiteSpace(_options.RetryQueue)
            ? $"{_options.Queue}.retry"
            : _options.RetryQueue;
    }

    private string ResolveDeadLetterExchangeName()
    {
        return string.IsNullOrWhiteSpace(_options.DeadLetterExchange)
            ? $"{_options.Queue}.dead-letter"
            : _options.DeadLetterExchange;
    }

    private string ResolveDeadLetterQueueName()
    {
        return string.IsNullOrWhiteSpace(_options.DeadLetterQueue)
            ? $"{_options.Queue}.dead-letter"
            : _options.DeadLetterQueue;
    }

    private string ResolveDeadLetterRoutingKey()
    {
        return string.IsNullOrWhiteSpace(_options.DeadLetterRoutingKey)
            ? ResolveDeadLetterQueueName()
            : _options.DeadLetterRoutingKey;
    }

    private int ResolveRetryDelayMilliseconds() => Math.Max(100, _options.RetryDelayMilliseconds);

    private int ResolveMaxRetryAttempts() => Math.Max(0, _options.MaxRetryAttempts);
}