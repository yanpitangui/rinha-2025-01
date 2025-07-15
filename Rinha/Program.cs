using System.Text.Json.Serialization;
using Akka.Actor;
using Akka.HealthCheck.Hosting.Web;
using Akka.Hosting;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Npgsql;
using Rinha;
using Rinha.Actors;
[module:DapperAot]

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddHttpClient("default", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("default")!));

builder.Services.AddHttpClient("fallback", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("fallback")!));
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2Support", true);
AppContext.SetSwitch("System.Globalization.Invariant", true);
builder.Services.RemoveAll<IHttpMessageHandlerBuilderFilter>();
DefaultTypeMap.MatchNamesWithUnderscores = true;
var connectionString = builder.Configuration.GetConnectionString("postgres");

// warmup
var warmupTasks = new List<Task>(50);
    
for (int i = 0; i < 50; i++)
{
    warmupTasks.Add(Task.Run(async () =>
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        const string sql = @"
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
            from = (DateTimeOffset?)null, to = (DateTimeOffset?)null
        });
        results.ToList();
    }));
}

await Task.WhenAll(warmupTasks);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

builder.Services.AddHealthChecks();

builder.Host.AddAkkaSetup();

var app = builder.Build();

app.MapPost("payments", ([FromBody] PaymentRequest request, [FromServices] IRequiredActor<RouterActor> decider) =>
{
    decider.ActorRef.Tell(request);
    return Results.Accepted();
});

app.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to) =>
{

    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    const string sql = @"
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

    var summary = new PaymentSummaryResponse(
        new PaymentSummaryItem(defaultResult.TotalRequests, defaultResult.TotalAmount),
        new PaymentSummaryItem(fallbackResult.TotalRequests, fallbackResult.TotalAmount)
    );
    return Results.Ok(summary);
});

app.MapPost("/purge-payments", async () =>
{
    await using var conn = new NpgsqlConnection(connectionString);
    await conn.OpenAsync();
    const string sql = "TRUNCATE TABLE payments";
    await conn.ExecuteAsync(sql);
});

app.MapAkkaHealthCheckRoutes();

app.Run();

[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
[JsonSerializable(typeof(HealthMonitorActor.ServiceHealth))]
[JsonSerializable(typeof(PaymentProcessorActor.ProcessorPaymentRequest))]
internal partial class JsonContext : JsonSerializerContext {}