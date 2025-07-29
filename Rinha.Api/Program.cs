using System.Text.Json;
using System.Threading.Channels;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Rinha.Common;
using StackExchange.Redis;
using CommandFlags = StackExchange.Redis.CommandFlags;

[module:DapperAot]


var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.ConfigureKestrel(options =>
{
    options.AddServerHeader = false;
    options.Limits.MaxRequestBodySize = 1024 * 8;
    options.Limits.MaxConcurrentConnections = 2048;
    options.Limits.MaxConcurrentUpgradedConnections = 1024;
    options.AllowSynchronousIO = false;
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

var connectionString = builder.Configuration.GetConnectionString("postgres");
var redis = builder.Configuration.GetConnectionString("redis");
var source = new NpgsqlDataSourceBuilder(connectionString).Build();
var app = builder.Build();
var redisChannel = Channel.CreateUnbounded<PaymentRequest>(new UnboundedChannelOptions() { SingleReader = true, SingleWriter = false });
app.MapPost("payments", ([FromBody] PaymentRequest request) =>
{
    _ = redisChannel.Writer.TryWrite(request);
    return Results.Accepted();
});

app.MapGet("/payments-summary", async ([FromQuery] DateTimeOffset? from, [FromQuery] DateTimeOffset? to) =>
{

    await using var conn = await source.OpenConnectionAsync();
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

app.MapPost("/purge-payments", async ([FromServices] NpgsqlDataSource source) =>
{
    await using var conn = await source.OpenConnectionAsync();
    const string sql = "TRUNCATE TABLE payments";
    await conn.ExecuteAsync(sql);
});

// warmup
var warmupTasks = new List<Task>(10);
    
for (int i = 0; i < 10; i++)
{
    warmupTasks.Add(Task.Run(async () =>
    {
        await using var conn = await source.OpenConnectionAsync();
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
    
    warmupTasks.Add(Task.Run(async () =>
    {
        var conn = await ConnectionMultiplexer.ConnectAsync(redis);
        await conn.GetDatabase().PingAsync();
    }));
}

await Task.WhenAll(warmupTasks);

Task.Run(async () =>
{
    var connection = await ConnectionMultiplexer.ConnectAsync(redis);
    var pub = connection.GetSubscriber();

    while (await redisChannel.Reader.WaitToReadAsync())
    {
        while (redisChannel.Reader.TryRead(out var msg))
        {
            await pub.PublishAsync("payments", JsonSerializer.SerializeToUtf8Bytes(msg, JsonContext.Default.PaymentRequest), CommandFlags.FireAndForget);
        }
    }
});

app.Run();
