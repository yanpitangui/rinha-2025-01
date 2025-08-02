using Akka.Actor;
using Akka.Hosting;
using Akka.Routing;
using Akka.Streams;
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
        
        var persisterConfig = builder.Configuration.GetSection("Persister");
        var persisterConfigOptions = persisterConfig.Get<PersisterConfig>()!;
        
        Console.WriteLine(persisterConfigOptions);

        builder.Services.AddAkka(actorSystemName, (b, provider) =>
        {
            b
                .WithActors((system, registry, resolver) =>
                {
                    var factory = resolver.GetService<IHttpClientFactory>();

                    var monitor = system.ActorOf(Props.Create<HealthMonitorActor>(factory));
                    registry.Register<HealthMonitorActor>(monitor);
                    var persister = new BatchPersister(source);
                    var defaultWriter = persister.StartStream(persisterConfigOptions, system.Materializer());
                    var fallbackWriter = persister.StartStream(persisterConfigOptions, system.Materializer());

                    
                    var defaultPool = system.ActorOf(Props
                        .Create<PaymentProcessorActor>("default", factory, defaultWriter)
                        .WithRouter(new RoundRobinPool(poolConfigOptions.DefaultPoolSize, new DefaultResizer(poolConfigOptions.DefaultPoolSize, 300))), "defaultPool");


                    var fallbackPool = system.ActorOf(Props
                        .Create<PaymentProcessorActor>("fallback", factory, fallbackWriter)
                        .WithRouter(new RoundRobinPool(poolConfigOptions.FallbackPoolSize,  new DefaultResizer(poolConfigOptions.FallbackPoolSize, 300))), "fallbackPool");

                    var router = system.ActorOf(
                        Props.Create<RouterActor>(registry.Get<HealthMonitorActor>(), defaultPool, fallbackPool)
                            .WithRouter(new RoundRobinPool(poolConfigOptions.RouterPoolSize,  new DefaultResizer(poolConfigOptions.RouterPoolSize, 300))),
                        "rinha");

                    registry.Register<RouterActor>(router);

                });
        });
        
    }
}