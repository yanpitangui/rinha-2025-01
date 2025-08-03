using Akka.Actor;
using Akka.Hosting;
using Akka.Routing;
using Npgsql;
using Rinha.Actors;

namespace Rinha;

public static class AkkaSetup
{
    public static void AddAkkaSetup(this IHostApplicationBuilder builder)
    {
        const string actorSystemName = "Rinha";

        var connectionString = builder.Configuration.GetConnectionString("postgres");
        var source = new NpgsqlDataSourceBuilder(connectionString).Build();

        var poolConfig = builder.Configuration.GetSection("Pool");
        var poolConfigOptions = poolConfig.Get<PoolConfig>()!;
        
        Console.WriteLine(poolConfigOptions);
        
        var pipelineSection = builder.Configuration.GetSection("Pipeline");
        var pipelineConfig = pipelineSection.Get<PipelineConfig>()!;
        
        Console.WriteLine(pipelineConfig);

        builder.Services.AddAkka(actorSystemName, (b, provider) =>
        {
            b
                .WithActors((system, registry, resolver) =>
                {
                    var factory = resolver.GetService<IHttpClientFactory>();

                    var monitor = system.ActorOf(Props.Create<HealthMonitorActor>(factory));
                    registry.Register<HealthMonitorActor>(monitor);
                    
                    var defaultPool = system.ActorOf(Props
                        .Create<PaymentPipelineActor>("default", factory, source, pipelineConfig));

                    var fallbackPool = system.ActorOf(Props
                        .Create<PaymentPipelineActor>("fallback", factory, source, pipelineConfig));

                    var router = system.ActorOf(
                        Props.Create<RouterActor>(registry.Get<HealthMonitorActor>(), defaultPool, fallbackPool)
                            .WithRouter(new RoundRobinPool(poolConfigOptions.RouterPoolSize,  new DefaultResizer(poolConfigOptions.RouterPoolSize, 300))),
                        "rinha");

                    registry.Register<RouterActor>(router);

                });
        });
        
    }
}