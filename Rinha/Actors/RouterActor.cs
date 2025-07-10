using Akka.Actor;
using Akka.Event;
using Akka.Routing;

namespace Rinha.Actors;

public sealed class RouterActor : ReceiveActor, IWithTimers
{
    private readonly IActorRef _monitor;
    private Switch _switch = Switch.Default;

    private readonly IActorRef _defaultPool;
    private readonly IActorRef _fallbackPool;

    private readonly ILoggingAdapter _log = Context.GetLogger();

    public RouterActor(IHttpClientFactory factory, IActorRef monitor, string connectionString)
    {
        _monitor = monitor;

        _defaultPool = Context.ActorOf(
            Props.Create<PaymentProcessorActor>("default", factory, connectionString)
                 .WithRouter(new RoundRobinPool(4)),
            "defaultPool");

        _fallbackPool = Context.ActorOf(
            Props.Create<PaymentProcessorActor>("fallback", factory, connectionString)
                 .WithRouter(new RoundRobinPool(4)),
            "fallbackPool");

        Receive<HealthMonitorActor.HealthUpdate>(update =>
        {
            _switch = Decide(update.Default, update.Fallback);
        });

        Receive<PaymentRequest>(req =>
        {
            if (_switch == Switch.Default)
            {
                _defaultPool.Tell(req);
            }
            else
            {
                _fallbackPool.Tell(req);
            }
        });

        Receive<PaymentProcessorActor.PaymentResult>(result =>
        {
            var retryCount = result.Request.RetryCount;

            if (retryCount >= 5)
            {
                _log.Debug("Request {0} permanently failed after retries", result.Request.CorrelationId);
                return;
            }

            var backoff = CalculateBackoff(retryCount);
            var retry = result.Request with { RetryCount = retryCount + 1 };

            _log.Debug("Retrying {0} in {1}", retry.CorrelationId, backoff);

            IActorRef target;
            if (result.Key == "default")
                target = _fallbackPool;
            else
                target = _defaultPool;

            Context.System.Scheduler.ScheduleTellOnce(
                delay: backoff,
                receiver: target,
                message: retry,
                sender: Self
            );
        });
    }

    private static TimeSpan CalculateBackoff(int retryCount)
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var maxDelay = TimeSpan.FromSeconds(5);
        var exponential = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, retryCount));
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100));
        return (exponential + jitter) > maxDelay ? maxDelay : exponential + jitter;
    }

    protected override void PreStart()
    {
        base.PreStart();
        _monitor.Tell(HealthMonitorActor.Commands.Subscribe.Instance);
    }

    private static Switch Decide(HealthMonitorActor.ServiceHealth main, HealthMonitorActor.ServiceHealth fallback)
    {
        if (main.Failing && fallback.Failing)
            return Switch.Default;

        if (!main.Failing && fallback.Failing)
            return Switch.Default;

        if (main.Failing && !fallback.Failing)
            return Switch.Fallback;

        var multiplier = 2.5m;
        return main.MinResponseTime <= fallback.MinResponseTime * multiplier
            ? Switch.Default
            : Switch.Fallback;
    }

    public ITimerScheduler Timers { get; set; } = null!;

    private enum Switch
    {
        Default,
        Fallback
    }
}
