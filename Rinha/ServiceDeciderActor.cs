using Akka.Actor;

namespace Rinha;

public sealed class ServiceDeciderActor : ReceiveActor, IWithTimers
{
    private readonly IHttpClientFactory _factory;
    private Switch _switch = Switch.Default;
    private readonly HttpClient _defaultClient;
    private readonly HttpClient _fallbackClient;
    private IActorRef _default;
    private IActorRef _fallback;

    public ServiceDeciderActor(IHttpClientFactory factory, IActorRef stats)
    {
        _factory = factory;
        _defaultClient = _factory.CreateClient("default");
        _fallbackClient = _factory.CreateClient("fallback");

        _default = Context.ActorOf(Props.Create<PaymentProcessorActor>("default", _defaultClient, stats));
        _fallback = Context.ActorOf(Props.Create<PaymentProcessorActor>("fallback", _fallbackClient, stats));
        
        ReceiveAsync<Commands.Init>(async _ =>
        {
            await Fetch();
            Timers.StartPeriodicTimer("Monitoring",  Commands.Fetch.Instance, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
            Sender.Tell(true);
        });
        
        Receive<Commands.Fetch>(_ =>
        {
            Fetch().PipeTo(Self);
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

    private async Task Fetch()
    {
        var mainTask = GetHealth(_defaultClient);
        var fallbackTask = GetHealth(_fallbackClient);
        await Task.WhenAll(mainTask, fallbackTask);
        var main = await mainTask ?? new ServiceHealth(true, decimal.MaxValue);
        var fallback = await fallbackTask ?? new ServiceHealth(true, decimal.MaxValue);

        _switch = Decide(main, fallback);
    }

    private static Switch Decide(ServiceHealth main, ServiceHealth fallback)
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
    
    private Task<ServiceHealth?> GetHealth(HttpClient client)
    {
        return client.GetFromJsonAsync<ServiceHealth>("/payments/service-health");
    }

    public ITimerScheduler Timers { get; set; } = null!;

    public static class Commands
    {
        public record Init
        {
            private Init(){}
            public static Init Instance { get; } = new();

        }

        public record Fetch
        {
            private Fetch(){}

            public static Fetch Instance { get; } = new();
        }
    }
    
    private sealed record ServiceHealth(bool Failing, decimal MinResponseTime);

    private enum Switch
    {
        Default,
        Fallback
    }
}

