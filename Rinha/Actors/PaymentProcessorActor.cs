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

    public PaymentProcessorActor(string key, IHttpClientFactory factory, string connectionString)
    {
        _key = key;
        _client = factory.CreateClient(key);
        _connectionString = connectionString;
        var writer = StartStream();
        Receive<PaymentRequest>(request => writer.Tell(request));
    }

    private IActorRef StartStream()
    {
        var (writer, source) = Source
            .ActorRef<PaymentRequest>(1000, OverflowStrategy.DropTail)
            .PreMaterialize(Context.System);
        
        source
            .SelectAsyncUnordered(15, RequestPayment)
            .To(Sink.Ignore<PaymentResult>())
            .Run(Context.Materializer());

        return writer;
    }

    private async Task<PaymentResult> RequestPayment(PaymentRequest request)
    {
        var requestedAt = DateTimeOffset.UtcNow;
        try
        {
            var response = await _client.PostAsJsonAsync("/payments", new
            {
                request.Amount,
                request.CorrelationId,
                requestedAt,
            });

            var result =  new PaymentResult(request, response.IsSuccessStatusCode, requestedAt, _key);
            if (result.IsSuccess)
            {
                await PersistToPostgres(result);
            }
            return result;
        }
        catch
        {
            return new PaymentResult(request, false, requestedAt, _key);
        }
    }

    private async Task PersistToPostgres(PaymentResult request)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = @"
        INSERT INTO payments (correlation_id, processor, amount, requested_at)
        VALUES (@CorrelationId, @Processor, @Amount, @RequestedAt);";
        
        await conn.ExecuteAsync(sql, new
        {
            request.Request.CorrelationId,
            processor = _key,
            request.Request.Amount,
            request.RequestedAt
        });
    }

    public sealed record PaymentResult(PaymentRequest Request, bool IsSuccess, DateTimeOffset RequestedAt, string Key);
} 
