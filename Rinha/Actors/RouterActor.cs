using Akka.Actor;
using Rinha.Common;

namespace Rinha.Actors;

public sealed class RouterActor : ReceiveActor
{
    private readonly IActorRef _monitor;
    private Switch _switch = Switch.Default;
    private const int MaxRetries = 5;

    private readonly IActorRef _defaultPool;
    private readonly IActorRef _fallbackPool;
    
    public RouterActor(
        IActorRef monitor,
        IActorRef defaultPool, 
        IActorRef fallbackPool)
    {
        _monitor = monitor;

        _defaultPool = defaultPool;

        _fallbackPool = fallbackPool;

        Receive<HealthMonitorActor.HealthUpdate>(update =>
        {
            _switch = Decide(update.Default, update.Fallback);
        });

        Receive<PaymentRequest>(req =>
        {
            if (req.Attempt >= MaxRetries)
            {
                return;
            }

            
            if (_switch == Switch.Default)
            {
                _defaultPool.Tell(req);
            }
            else
            {
                _fallbackPool.Tell(req);
            }
        });
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

        return Switch.Default;
    }
    
    private enum Switch
    {
        Default,
        Fallback
    }
}
