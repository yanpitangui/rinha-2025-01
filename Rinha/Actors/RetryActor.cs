using System.Security.Cryptography;
using Akka.Actor;
using Rinha.Common;

namespace Rinha.Actors;

public class RetryActor : ReceiveActor
{
    private static readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();

    public RetryActor()
    {
        Receive<Commands.RetryablePayment>(o =>
        {
            var delay = ComputeBackoffWithJitter(o.Request.Attempt);
            Context.System.Scheduler.ScheduleTellOnce(delay, o.Sender, o.Request, Self);
        });
    }
    
    private static TimeSpan ComputeBackoffWithJitter(int attempt)
    {
        var baseDelayMs = (int)(500 * Math.Pow(2, attempt)); // 200ms, 400ms, 800ms
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

    public static class Commands
    {
        public sealed record RetryablePayment(PaymentRequest Request, IActorRef Sender);
    }
    
}