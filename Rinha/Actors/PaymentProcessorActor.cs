using System.Threading.Channels;
using Akka.Actor;

namespace Rinha.Actors;

public sealed class PaymentProcessorActor : ActorBase, IWithTimers
{
    private readonly string _key;
    private readonly ChannelWriter<BatchPersister.Commands.PersistPayment> _persister;
    private readonly HttpClient _client;

    public PaymentProcessorActor(string key, IHttpClientFactory factory, ChannelWriter<BatchPersister.Commands.PersistPayment> persister)
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
                _persister.TryWrite(new BatchPersister.Commands.PersistPayment
                    (request.CorrelationId, request.Amount, requestedAt, _key));
            }
            else
            {
                Timers.StartSingleTimer(request.CorrelationId, request, TimeSpan.FromSeconds(1));
                Self.Tell(request);
            }

        }
        catch
        {
            Timers.StartSingleTimer(request.CorrelationId, request, TimeSpan.FromSeconds(1));
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
        
        Task.Run(() => RequestPayment(request));
        return true;
    }

    public ITimerScheduler Timers { get; set; } = null!;
} 
