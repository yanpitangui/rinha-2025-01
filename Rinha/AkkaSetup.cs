using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.HealthCheck.Hosting;
using Akka.HealthCheck.Hosting.Web;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Akka.Routing;
using Akka.Streams;
using Rinha.Actors;

namespace Rinha;

public static class AkkaSetup
{
    public static void AddAkkaSetup(this IHostBuilder builder)
    {
        const string actorSystemName = "Rinha";
        builder.ConfigureServices((ctx, services) =>
        {
            var connectionString = ctx.Configuration.GetConnectionString("postgres");

            var clusterConfig = ctx.Configuration.GetSection("Cluster");
            var clusterConfigOptions = clusterConfig.Get<ClusterOptions>();
            
            var poolConfig = ctx.Configuration.GetSection("Pool");
            var poolConfigOptions = poolConfig.Get<PoolConfig>()!;
            
            Console.WriteLine(poolConfigOptions);
            
            var persisterConfig = ctx.Configuration.GetSection("Persister");
            var persisterConfigOptions = persisterConfig.Get<PersisterConfig>()!;
            
            Console.WriteLine(persisterConfigOptions);

            services.WithAkkaHealthCheck(HealthCheckType.Cluster | HealthCheckType.Default);
            services.AddAkka(actorSystemName, (b, provider) =>
            {
                var clusterOptions = new Akka.Cluster.Hosting.ClusterOptions
                {
                    MinimumNumberOfMembers = 1,
                    SeedNodes = clusterConfigOptions.Seeds,
                    Roles = new[] { actorSystemName }
                };
    
                b.WithClustering(clusterOptions)
                    .WithRemoting(opt =>
                    {
                        opt.PublicHostName = clusterConfigOptions.Ip;
                        opt.PublicPort = clusterConfigOptions.Port;
                        opt.HostName = "0.0.0.0";
                        opt.Port = clusterConfigOptions.Port;
                    })
                .WithSingleton<HealthMonitorActor>("health-monitor",
                (_,ar,p) =>
                    Props.Create<HealthMonitorActor>(p.GetService<IHttpClientFactory>()), new ClusterSingletonOptions
                {
                    Role = actorSystemName,
                })
                .WithActors((system, registry, resolver) =>
                    {
                        var factory = resolver.GetService<IHttpClientFactory>();

                        var persister = new BatchPersister(connectionString);
                        var writer = persister.StartStream(persisterConfigOptions, system.Materializer());
                        
                        var defaultPool = system.ActorOf(Props
                            .Create<PaymentProcessorActor>("default", factory, writer)
                            .WithRouter(new RoundRobinPool(poolConfigOptions.DefaultPoolSize)), "defaultPool");
                        
                        
                        var fallbackPool = system.ActorOf(Props
                            .Create<PaymentProcessorActor>("fallback", factory, writer)
                            .WithRouter(new RoundRobinPool(poolConfigOptions.FallbackPoolSize)), "fallbackPool");
                        
                        var router = system.ActorOf(
                            Props.Create<RouterActor>(registry.Get<HealthMonitorActor>(), defaultPool, fallbackPool)
                                .WithRouter(new RoundRobinPool(poolConfigOptions.RouterPoolSize)),
                            "rinha");

                        registry.Register<RouterActor>(router);

                }).WithWebHealthCheck(provider);
                
            });
        });
    }
}