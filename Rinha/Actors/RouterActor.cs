using Akka.Actor;

namespace Rinha.Actors;

public sealed class RouterActor : ReceiveActor, IWithTimers
{
    private readonly IActorRef _monitor;
    private Switch _switch = Switch.Default;
    private readonly IActorRef _default;
    private readonly IActorRef _fallback;

    public RouterActor(IHttpClientFactory factory, IActorRef monitor, string connectionString)
    {
        _monitor = monitor;
        _default = Context.ActorOf(Props.Create<PaymentProcessorActor>("default", factory, connectionString));
        _fallback = Context.ActorOf(Props.Create<PaymentProcessorActor>("fallback", factory, connectionString));

        Receive<HealthMonitorActor.HealthUpdate>(update =>
        {
            _switch = Decide(update.Default, update.Fallback);
        });
        
        Receive<PaymentRequest>(req =>
        {
            if (_switch == Switch.Default)
            {
                _default.Forward(req);
            }
            else
            {
                _fallback.Forward(req);
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

