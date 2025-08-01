using System.Text.Json;
using Akka.Actor;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;
using Polly;
using Polly.Extensions.Http;
using Rinha;
using Rinha.Actors;
using Rinha.Common;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient("default", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("default")!))
    .AddPolicyHandler(GetRetryPolicy());

builder.Services.AddHttpClient("fallback", o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString("fallback")!))
    .AddPolicyHandler(GetRetryPolicy());

builder.Services.RemoveAll<IHttpMessageHandlerBuilderFilter>();
var nats = builder.Configuration.GetConnectionString("nats");

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

var natsConnection = new NatsConnection(new NatsOpts
{
    Url = nats,
    SerializerRegistry = new NatsJsonContextSerializerRegistry(JsonContext.Default),
});

INatsJSContext js = natsConnection.CreateJetStreamContext();

var stream = await js.CreateOrUpdateStreamAsync(new StreamConfig(name: "payments", subjects: ["payments.>"]));


var app = builder.Build();
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStarted.Register(async () =>
{
    var registry = app.Services.GetRequiredService<ActorRegistry>();
    var router = registry.Get<RouterActor>();
    INatsJSConsumer consumer = await stream.CreateOrUpdateConsumerAsync(new ConsumerConfig("payments"));
    while (true)
    {
        const int batchSize = 100;

        await foreach (var msg in consumer.ConsumeAsync<PaymentRequest>(opts: new NatsJSConsumeOpts
                           { MaxMsgs = batchSize }))
        {
            await msg.AckAsync();
            router.Tell(msg.Data!);
        }
    }

});

app.Run();