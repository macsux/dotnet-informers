using System;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;

namespace informers
{
    /// <summary>
    ///     Runs <see cref="DeploymentReadinessController" /> as one of several replicas: every replica keeps its informers warm, only the holder
    ///     of the Lease reconciles.
    /// </summary>
    public class ReadinessControllerService : BackgroundService
    {
        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;
        private readonly IConfiguration _configuration;
        private readonly ILoggerFactory _loggerFactory;

        public ReadinessControllerService(IKubernetesInformerFactory informers, IKubernetes kubernetes, IConfiguration configuration, ILoggerFactory loggerFactory)
        {
            _informers = informers;
            _kubernetes = kubernetes;
            _configuration = configuration;
            _loggerFactory = loggerFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var ns = _configuration["Namespace"] ?? "default";
            var identity = _configuration["LeaderElection:Identity"] ?? Environment.MachineName;
            using var election = new KubernetesLeaderElection(_kubernetes, ns, _configuration["LeaderElection:Lease"] ?? "readiness-controller", identity,
                leaseDuration: TimeSpan.FromSeconds(6), renewDeadline: TimeSpan.FromSeconds(4), retryPeriod: TimeSpan.FromSeconds(1),
                logger: _loggerFactory.CreateLogger<KubernetesLeaderElection>());
            var controller = new DeploymentReadinessController(_informers, _kubernetes) { Identity = identity };
            await controller.RunAsync(ns, new RunOptions
            {
                MaxConcurrency = 4,
                Name = nameof(DeploymentReadinessController),
                LeaderElection = election,
                Logger = _loggerFactory.CreateLogger<DeploymentReadinessController>(),
            }, stoppingToken);
        }
    }
}
