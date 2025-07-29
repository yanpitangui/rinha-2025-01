using System.Text.Json;
using Akka.Actor;
using Akka.Hosting;
using Dapper;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using Polly;
using Polly.Extensions.Http;
using Rinha;
using Rinha.Actors;
using Rinha.Common;
using StackExchange.Redis;


HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient("default", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("default")!))
    .AddPolicyHandler(GetRetryPolicy());

builder.Services.AddHttpClient("fallback", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("fallback")!))
    .AddPolicyHandler(GetRetryPolicy());

builder.Services.RemoveAll<IHttpMessageHandlerBuilderFilter>();
DefaultTypeMap.MatchNamesWithUnderscores = true;

var redis = builder.Configuration.GetConnectionString("redis");
var muxxer = ConnectionMultiplexer.Connect(redis, options =>
{
    options.AbortOnConnectFail = false;
    options.ConnectRetry = 10;
});

static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2,
            retryAttempt)));
}

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
});

builder.AddAkkaSetup();

var app = builder.Build();
var registry = app.Services.GetRequiredService<ActorRegistry>();
var router = registry.Get<RouterActor>();

var readTask = Task.Run(async () =>
{
    var subscriber = muxxer.GetSubscriber();
    subscriber.Subscribe(RedisChannel.Literal("payment"), (_, value) =>
    {
        var paymentRequest = JsonSerializer.Deserialize<PaymentRequest>(value.ToString(), JsonContext.Default.Options);
        router.Tell(paymentRequest);
    });
});

app.Run();