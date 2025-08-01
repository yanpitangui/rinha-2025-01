using System.Buffers;
using System.Threading.Channels;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using Npgsql;
using Rinha.Api;
using Rinha.Common;

[module:DapperAot]


var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.ConfigureKestrel(options =>
{
    options.AddServerHeader = false;
    options.Limits.MaxRequestBodySize = 1024;
    options.Limits.MaxConcurrentConnections = 2048;
    options.Limits.MaxConcurrentUpgradedConnections = 1024;
    options.AllowSynchronousIO = false;
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

var connectionString = builder.Configuration.GetConnectionString("postgres");
var nats = builder.Configuration.GetConnectionString("nats");
var source = new NpgsqlDataSourceBuilder(connectionString).Build();
var app = builder.Build();
var natsChannel = Channel.CreateUnbounded<MemoryStreamManager.PooledBuffer>(new UnboundedChannelOptions() { SingleReader = false, SingleWriter = false });
app.MapPost("/payments", async context =>
{
    var length = (int)context.Request.ContentLength;
    byte[] rented = ArrayPool<byte>.Shared.Rent(length);
    
    await context.Request.Body.ReadExactlyAsync(rented.AsMemory(0, length));
    
    var pooledBuffer = new MemoryStreamManager.PooledBuffer(rented, length);
    natsChannel.Writer.TryWrite(pooledBuffer);
    context.Response.StatusCode = StatusCodes.Status202Accepted;
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

app.MapPost("/purge-payments", async () =>
{
    await using var conn = await source.OpenConnectionAsync();
    const string sql = "TRUNCATE TABLE payments";
    await conn.ExecuteAsync(sql);
});

// warmup
var warmupTasks = new List<Task>(10);

var natsConnection = new NatsConnection(new NatsOpts
{
    Url = nats
});

INatsJSContext js = natsConnection.CreateJetStreamContext();

await js.CreateOrUpdateStreamAsync(new StreamConfig(name: "payments", subjects: ["payments"]));


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
        await natsConnection.PingAsync();
    }));
}

await Task.WhenAll(warmupTasks);


for (int i = 0; i < Environment.ProcessorCount; i++)
{
    Task.Run(async () =>
    {
        while (await natsChannel.Reader.WaitToReadAsync())
        {
            while (natsChannel.Reader.TryRead(out var msg))
            {
                try
                {
                    await js.PublishConcurrentAsync($"payments.{Guid.NewGuid()}", msg.Buffer.AsMemory(0, msg.Length));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(msg.Buffer);
                }
            }
        }
    });
}

app.Run();


