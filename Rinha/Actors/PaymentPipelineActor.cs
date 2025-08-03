using System.Security.Cryptography;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Npgsql;
using Rinha.Common;

namespace Rinha.Actors;

public sealed class PaymentPipelineActor : ReceiveActor
{
    private const int MaxRetries = 3;
    private static readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();

    private readonly string _key;
    private readonly NpgsqlDataSource _source;
    private readonly ChannelWriter<PaymentRequest> _pipeline;
    private readonly HttpClient _client;
    
    public PaymentPipelineActor(string key, IHttpClientFactory factory, NpgsqlDataSource source, PipelineConfig pipelineConfig)
    {
        _key = key;
        _source = source;
        _pipeline = StartStream(pipelineConfig);
        _client = factory.CreateClient(key);
        ReceiveAsync<PaymentRequest>(async msg =>
        {
            await _pipeline.WriteAsync(msg);
        });

    }
    
    public ChannelWriter<PaymentRequest> StartStream(PipelineConfig config)
    {
        var materializer = Context.Materializer();
        var (mainWriter, mainSource) = Source.Channel<PaymentRequest>
                (10000, fullMode: BoundedChannelFullMode.Wait)
            .PreMaterialize(materializer);
        mainSource
            .SelectAsync(config.HandlePaymentParallelism, HandlePayment)
            .Where(x => x.Success)
            .GroupedWithin(config.GroupSize, TimeSpan.FromMilliseconds(config.Timeout))
            .SelectAsync(config.PersistPaymentsParallelism, PersistPayments)
            .To(Sink.Ignore<List<PaymentResult>>())
            .Run(materializer);

        return mainWriter;
    }

    private async Task<PaymentResult> HandlePayment(PaymentRequest payment)
    {
        var requestedAt = DateTimeOffset.UtcNow;

        try
        {
            var response = await _client.PostAsJsonAsync("/payments", new ProcessorPaymentRequest(
                payment.Amount,
                requestedAt,
                payment.CorrelationId), WorkerContext.Default.ProcessorPaymentRequest);

            if (response.IsSuccessStatusCode)
            {
                var persisted = new PaymentResult(
                    payment.CorrelationId,
                    payment.Amount,
                    requestedAt,
                    true
                );
                return persisted;
            }
        }
        catch
        {
            // ignored
        }
        return new PaymentResult(payment.CorrelationId, payment.Amount, requestedAt, false);
    }

    private void RetryOrGiveUp(PaymentRequest payment, IActorRef sender)
    {
        if (payment.Attempt >= MaxRetries)
        {
            return;
        }

        var nextAttempt = payment.Attempt + 1;
        var delay = ComputeBackoffWithJitter(nextAttempt);

        var retryMessage = payment with { Attempt = nextAttempt };
        Context.System.Scheduler.ScheduleTellOnce(delay, sender, retryMessage, Self);
        return;
        
    }

    private static TimeSpan ComputeBackoffWithJitter(int attempt)
    {
        var baseDelayMs = (int)(100 * Math.Pow(2, attempt)); // 200ms, 400ms, 800ms
        var jitter = RandomJitterMilliseconds(20); // ±20ms
        return TimeSpan.FromMilliseconds(baseDelayMs + jitter);
    }

    private static int RandomJitterMilliseconds(int maxJitter)
    {
        Span<byte> bytes = stackalloc byte[4];
        _rng.GetBytes(bytes);
        int raw = BitConverter.ToInt32(bytes) & int.MaxValue; // force positive
        return raw % (2 * maxJitter + 1) - maxJitter;
    }
    
    private async Task<List<PaymentResult>> PersistPayments(IEnumerable<PaymentResult> batch)
    {
        await using var conn = await _source.OpenConnectionAsync();

        var batchList = batch.ToList();

        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY payments (correlation_id, processor, amount, requested_at) FROM STDIN (FORMAT BINARY)");
        foreach (var payment in batchList)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(payment.CorrelationId);
            await writer.WriteAsync(_key);
            await writer.WriteAsync(payment.Amount);
            await writer.WriteAsync(payment.RequestedAt);
        }
        await writer.CompleteAsync();
        return batchList;
        
    }
    
    private sealed record PaymentResult(Guid CorrelationId, decimal Amount, DateTimeOffset RequestedAt, bool Success);

    public sealed record ProcessorPaymentRequest(decimal Amount, DateTimeOffset RequestedAt, Guid CorrelationId);

}
