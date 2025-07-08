using Akka.Actor;
using Akka.Hosting;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Npgsql;
using Rinha;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddHttpClient("default", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("default")!));

builder.Services.AddHttpClient("fallback", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("fallback")!));

builder.Services.RemoveAll<IHttpMessageHandlerBuilderFilter>();
Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
var connectionString = builder.Configuration.GetConnectionString("Postgres");

builder.Services.AddAkka("rinha", (b, provider) =>
{
    b.WithActors((system, registry) =>
    {
        var decider = system.ActorOf(Props.Create<ServiceDeciderActor>
            (provider.GetRequiredService<IHttpClientFactory>(), connectionString), "rinha");
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

app.MapGet("/payments-summary", async ([FromServices] IConfiguration config, [FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to) =>
{

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    var sql = @"
                SELECT processor,
                       COUNT(*) AS total_requests,
                       SUM(amount) AS total_amount
                FROM payments
                WHERE (@from IS NULL OR requested_at >= @from)
                  AND (@to IS NULL OR requested_at <= @to)
                GROUP BY processor;
            ";

    var results = await conn.QueryAsync<SummaryRow>(sql, new
    {
        from, to
    });

    var defaultResult = results.FirstOrDefault(r => r.Processor == "default") ?? new SummaryRow("default", 0, 0);
    var fallbackResult = results.FirstOrDefault(r => r.Processor == "fallback") ?? new SummaryRow("fallback", 0, 0);

    return Results.Ok(new PaymentSummaryResponse(
        new PaymentSummaryItem(defaultResult.TotalRequests, defaultResult.TotalAmount),
        new PaymentSummaryItem(fallbackResult.TotalRequests, fallbackResult.TotalAmount)
    ));
});

app.Run();


public record PaymentRequest(Guid CorrelationId, decimal Amount);
public record PaymentSummaryResponse(PaymentSummaryItem Default, PaymentSummaryItem Fallback);
public record PaymentSummaryItem(int TotalRequests, decimal TotalAmount);

public sealed record SummaryRow
{
    public SummaryRow() {}
    public SummaryRow(string Processor, int TotalRequests, decimal TotalAmount)
    {
        this.Processor = Processor;
        this.TotalRequests = TotalRequests;
        this.TotalAmount = TotalAmount;
    }

    public string Processor { get; init; }
    public int TotalRequests { get; init; }
    public decimal TotalAmount { get; init; }
}
