using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Remote.Hosting;
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
            
            services.AddAkka(actorSystemName, (b, provider) =>
            {
                var remoteOptions = new RemoteOptions
                {
                    HostName = clusterConfigOptions!.Ip,
                    Port = clusterConfigOptions.Port, 
                };
                var clusterOptions = new Akka.Cluster.Hosting.ClusterOptions
                {
                    MinimumNumberOfMembers = 1,
                    SeedNodes = clusterConfigOptions.Seeds,
                    Roles = new[] { actorSystemName }
                };
    
                b.WithClustering(clusterOptions)
                    .WithRemoting(remoteOptions)
                .WithSingleton<HealthMonitorActor>("health-monitor",
                    (_,ar,p) =>
                        Props.Create<HealthMonitorActor>(p.GetService<IHttpClientFactory>()), new ClusterSingletonOptions
                    {
                        Role = actorSystemName,
                    })
                    .WithActors((system, registry) =>
                    {
                        var decider = system.ActorOf(Props.Create<RouterActor>
                            (provider.GetRequiredService<IHttpClientFactory>(), registry.Get<HealthMonitorActor>(), connectionString), "rinha");
                        registry.Register<RouterActor>(decider);
                    });
                
            });
        });
    }
}