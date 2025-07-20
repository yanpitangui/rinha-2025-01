using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Dapper;
using Npgsql;

namespace Rinha.Actors;

public sealed class PaymentProcessorActor : ActorBase
{
    private readonly string _key;
    private readonly IActorRef _persister;
    private readonly HttpClient _client;

    public PaymentProcessorActor(string key, IHttpClientFactory factory, IActorRef persister)
    {
        _key = key;
        _persister = persister;
        _client = factory.CreateClient(key);
    }
    
    private async Task RequestPayment(PaymentRequest request)
    {
        var requestedAt = DateTimeOffset.UtcNow;
        try
        {
            var response = await _client.PostAsJsonAsync("/payments", new ProcessorPaymentRequest
            (
                request.Amount,
                requestedAt,
                request.CorrelationId
            ), JsonContext.Default.ProcessorPaymentRequest);

            if (response.IsSuccessStatusCode)
            {
                _persister.Tell(new BatchPersisterActor.Commands.PersistPayment
                    (request.CorrelationId, request.Amount, requestedAt, _key));
            }
            else
            {
                Self.Tell(request);
            }

        }
        catch
        {
            Self.Tell(request);
        }
    }

    public sealed record ProcessorPaymentRequest(decimal Amount, DateTimeOffset RequestedAt, Guid CorrelationId);

    protected override bool Receive(object message)
    {
        var request = message switch
        {
            PaymentRequest p => p,
            _ => throw new ArgumentOutOfRangeException(nameof(message), message, null)
        };
        
        RequestPayment(request).PipeTo(Self);
        
        return true;
    }
} 
