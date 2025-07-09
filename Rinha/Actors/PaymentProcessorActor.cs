using System.Threading.Channels;
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
        Receive<PaymentRequest>(request => writer.WriteAsync(request).PipeTo(Self));
    }

    private ChannelWriter<PaymentRequest> StartStream()
    {
        var (writer, source) = Source.Channel<PaymentRequest>(1000).PreMaterialize(Context.System);

        source
            .SelectAsyncUnordered(10, RequestPayment)
            .Where(result => result.IsSuccess)
            .SelectAsync(5, PersistToPostgres)
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

            return new PaymentResult(request, response.IsSuccessStatusCode, requestedAt);
        }
        catch
        {
            return new PaymentResult(request, false, requestedAt);
        }
    }

    private async Task<PaymentResult> PersistToPostgres(PaymentResult request)
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
        
        return request;
    }

    private sealed record PaymentResult(PaymentRequest Request, bool IsSuccess, DateTimeOffset RequestedAt);
} 
