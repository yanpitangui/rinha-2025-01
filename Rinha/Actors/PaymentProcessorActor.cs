using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Dapper;
using Npgsql;

namespace Rinha.Actors;

public sealed class PaymentProcessorActor : ReceiveActor
{
    private readonly string _key;
    private readonly HttpClient _client;
    private readonly string _connectionString;

    public PaymentProcessorActor(string key, IHttpClientFactory factory, ProcessorConfig config, string connectionString)
    {
        _key = key;
        _client = factory.CreateClient(key);
        _connectionString = connectionString;
        var writer = StartStream(config);
        Receive<PaymentRequest>(request => writer.Tell(request));
    }

    private IActorRef StartStream(ProcessorConfig config)
    {
        var materializer = Context.Materializer();
        var (mainWriter, mainSource) = Source
            .ActorRef<object>(1000, OverflowStrategy.DropTail)
            .PreMaterialize(materializer);
        
        var failureSink = Sink.ActorRef<PaymentResult>(
            mainWriter,
            onCompleteMessage: null
        );
        
        mainSource
            .SelectAsyncUnordered(config.RequestPaymentParallelism, RequestPayment)
            .DivertTo(failureSink, (result) => !result.IsSuccess)
            .GroupedWithin(config.GroupSize, TimeSpan.FromMilliseconds(config.Timeout))
            .SelectAsync(config.PersistPaymentsParallelism, PersistPayments)
            .To(Sink.Ignore<List<PaymentResult>>())
            .Run(materializer);

        return mainWriter;
    }

    private async Task<PaymentResult> RequestPayment(object req)
    {
        var request = req switch
        {
            PaymentRequest p => p,
            PaymentResult r => r.Request,
            _ => throw new ArgumentOutOfRangeException(nameof(req), req, null)
        };
        var requestedAt = DateTimeOffset.UtcNow;
        try
        {
            var response = await _client.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest);

            var result =  new PaymentResult(request, response.IsSuccessStatusCode, requestedAt);
            return result;
        }
        catch
        {
            return new PaymentResult(request, false, requestedAt);
        }
    }

    private async Task<List<PaymentResult>> PersistPayments(IEnumerable<PaymentResult> batch)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        var batchList = batch.ToList();
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY payments (correlation_id, processor, amount, requested_at) FROM STDIN (FORMAT BINARY)");
        foreach (var payment in batchList)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(payment.Request.CorrelationId);
            await writer.WriteAsync(_key); // processor
            await writer.WriteAsync(payment.Request.Amount);
            await writer.WriteAsync(payment.RequestedAt);
        }
        await writer.CompleteAsync();
        
        return batchList;
        
    }

    public sealed record ProcessorPaymentRequest(decimal Amount, DateTimeOffset RequestedAt, Guid CorrelationId);
    public sealed record PaymentResult(PaymentRequest Request, bool IsSuccess, DateTimeOffset RequestedAt);
} 
