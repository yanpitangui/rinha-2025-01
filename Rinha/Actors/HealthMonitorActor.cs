using System.Text.Json;
using Akka.Actor;

namespace Rinha.Actors;

public class HealthMonitorActor : ReceiveActor, IWithTimers
{
    private List<IActorRef> _subscribers = new();
    private readonly HttpClient _default;
    private readonly HttpClient _fallback;
    public ITimerScheduler Timers { get; set; } = null!;
    private ServiceHealth _defaultHealth = new();
    private ServiceHealth _fallbackHealth = new();

    public HealthMonitorActor(IHttpClientFactory factory)
    {
        _default = factory.CreateClient("default");
        _fallback = factory.CreateClient("fallback");

        Receive<Commands.Subscribe>(_ =>
        {
            _subscribers.Add(Sender);
            Sender.Tell(new HealthUpdate(_defaultHealth, _fallbackHealth));
        });

        ReceiveAsync<Commands.Fetch>(async _ =>
        {
            await Fetch();
            foreach (var subscriber in _subscribers)
            {
                subscriber.Tell(new HealthUpdate(_defaultHealth, _fallbackHealth));
            }
        });
    }

    protected override void PreStart()
    {
        base.PreStart();
        Fetch().PipeTo(Self);
    }
    
    private Task<ServiceHealth?> GetHealth(HttpClient client)
    {
        return client.GetFromJsonAsync<ServiceHealth>("/payments/service-health", JsonContext.Default.ServiceHealth);
    }

    private async Task Fetch()
    {
        var mainTask = GetHealth(_default);
        var fallbackTask = GetHealth(_fallback);
        await Task.WhenAll(mainTask, fallbackTask);
        _defaultHealth = await mainTask ?? new ServiceHealth(true, decimal.MaxValue);
        _fallbackHealth =  await fallbackTask ?? new ServiceHealth(true, decimal.MaxValue);
        Timers.StartPeriodicTimer("monitoring", Commands.Fetch.Instance, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }


    public static class Commands
    {
        public sealed record Subscribe
        {
            private Subscribe() {}
            public static Subscribe Instance { get; } = new();
        }
        
        public record Fetch
        {
            private Fetch(){}

            public static Fetch Instance { get; } = new();
        }
    }
    
    public sealed record HealthUpdate(ServiceHealth Default, ServiceHealth Fallback);
    
    public sealed record ServiceHealth
    {
        public ServiceHealth() : this(false, 0){}
        public ServiceHealth(bool Failing, decimal MinResponseTime)
        {
            this.Failing = Failing;
            this.MinResponseTime = MinResponseTime;
        }

        public bool Failing { get; init; }
        public decimal MinResponseTime { get; init; }
        
    }
}