using Akka.Actor;
using Akka.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Rinha;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddHttpClient("default", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("default")!));

builder.Services.AddHttpClient("fallback", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("fallback")!));

builder.Services.RemoveAll<IHttpMessageHandlerBuilderFilter>();

builder.Services.AddAkka("rinha", (b, provider) =>
{
    b.WithActors((system, registry) =>
    {
        var stats = system.ActorOf(Props.Create<PaymentStatsActor>());
        registry.Register<PaymentStatsActor>(stats);
        
        var decider = system.ActorOf(Props.Create<ServiceDeciderActor>
            (provider.GetRequiredService<IHttpClientFactory>(), stats), "rinha");
        registry.Register<ServiceDeciderActor>(decider);


    });
    b.AddStartup(async (s, r) =>
    {
        await r.Get<ServiceDeciderActor>()
            .Ask(ServiceDeciderActor.Commands.Init.Instance);
    });
});

var app = builder.Build();

app.MapPost("payments", ([FromBody] PaymentRequest request, [FromServices] IRequiredActor<ServiceDeciderActor> decider) =>
{
    decider.ActorRef.Tell(request);
    return Results.Accepted();
});

app.MapGet("payments-summary", ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to, [FromServices] IRequiredActor<PaymentStatsActor> stats) => 
    stats.ActorRef.Ask<PaymentSummaryResponse>(new PaymentStatsActor.Commands.GetPaymentSummary(from, to)));
app.Run();


public record PaymentRequest(Guid CorrelationId, decimal Amount);