using System.Threading.Channels;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Npgsql;
using Rinha.Common;

namespace Rinha.Actors;

public sealed class PaymentPipelineActor : ReceiveActor
{
    private readonly string _key;
    private readonly NpgsqlDataSource _source;
    private readonly ChannelWriter<(PaymentRequest, IActorRef)> _pipeline;
    private readonly HttpClient _client;
    private readonly IActorRef _retryActor;
    
    public PaymentPipelineActor(string key, IHttpClientFactory factory, NpgsqlDataSource source, PipelineConfig pipelineConfig)
    {
        _key = key;
        _source = source;
        _pipeline = StartStream(pipelineConfig);
        _client = factory.CreateClient(key);
        _retryActor = Context.ActorOf(Props.Create<RetryActor>());
        ReceiveAsync<PaymentRequest>(async msg =>
        {
            var sender = Sender;
            await _pipeline.WriteAsync((msg, sender));
        });

    }
    
    public ChannelWriter<(PaymentRequest, IActorRef)> StartStream(PipelineConfig config)
    {
        var materializer = Context.Materializer();
        var (mainWriter, mainSource) = Source.Channel<(PaymentRequest, IActorRef)>
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

    private async Task<PaymentResult> HandlePayment((PaymentRequest payment, IActorRef sender) req)
    {
        var requestedAt = DateTimeOffset.UtcNow;

        try
        {
            var response = await _client.PostAsJsonAsync("/payments", new ProcessorPaymentRequest(
                req.payment.Amount,
                requestedAt,
                req.payment.CorrelationId), WorkerContext.Default.ProcessorPaymentRequest);

            if (response.IsSuccessStatusCode)
            {
                var persisted = new PaymentResult(
                    req.payment.CorrelationId,
                    req.payment.Amount,
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
        RetryOrGiveUp(req.payment, req.sender);
        return new PaymentResult(req.payment.CorrelationId, req.payment.Amount, requestedAt, false);
    }

    private void RetryOrGiveUp(PaymentRequest payment, IActorRef sender)
    {
        var nextAttempt = payment.Attempt + 1;
        var retryMessage = payment with { Attempt = nextAttempt };
        _retryActor.Tell(new RetryActor.Commands.RetryablePayment(retryMessage, sender));
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
