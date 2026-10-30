using k8s;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.KubernetesCore;

namespace informers
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        // dotnet run                                    pod change log for the "default" namespace
        // dotnet run -- --Controller=readiness --Namespace=demo --LeaderElection:Identity=replica-a
        //                                               deployment readiness controller, leader-elected through a Lease
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureLogging(x => x.AddConsole())
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddKubernetesClient(KubernetesClientConfiguration.BuildDefaultConfig);
                    services.AddKubernetesInformer<k8s.Models.V1Pod>(pods => pods.StripManagedFields());
                    services.AddKubernetesInformers();

                    if (hostContext.Configuration["Controller"] == "readiness")
                    {
                        services.AddHostedService<ReadinessControllerService>();
                    }
                    else
                    {
                        services.AddHostedService<ControllerService>();
                        services.AddSingleton<IController, DeltaChangesQueryingController>();
                    }
                });
    }
}
