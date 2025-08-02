using System.Security.Cryptography;
using System.Threading.Channels;
using Akka.Actor;
using Rinha.Common;

namespace Rinha.Actors;

public sealed class PaymentProcessorActor : ReceiveActor
{
    private const int MaxRetries = 3;
    private static readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();

    private readonly string _key;
    private readonly ChannelWriter<BatchPersister.Commands.PersistPayment> _persister;
    private readonly HttpClient _client;
    
    public PaymentProcessorActor(string key, IHttpClientFactory factory, ChannelWriter<BatchPersister.Commands.PersistPayment> persister)
    {
        _key = key;
        _persister = persister;
        _client = factory.CreateClient(key);
        ReceiveAsync<PaymentRequest>(msg =>
        {
            var sender = Sender;
            return HandlePayment(msg, sender);
        });

    }

    private async Task HandlePayment(PaymentRequest payment, IActorRef sender)
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
                var persisted = new BatchPersister.Commands.PersistPayment(
                    payment.CorrelationId,
                    payment.Amount,
                    requestedAt,
                    _key,
                    new TaskCompletionSource()
                );
                await _persister.WriteAsync(persisted);
                await persisted.Tcs.Task;
            }
            else
            {
                RetryOrGiveUp(payment, sender);
            }
        }
        catch
        {
            RetryOrGiveUp(payment, sender);
        }
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
    public sealed record ProcessorPaymentRequest(decimal Amount, DateTimeOffset RequestedAt, Guid CorrelationId);

}
