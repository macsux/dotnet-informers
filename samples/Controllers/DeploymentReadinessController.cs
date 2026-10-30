using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>
    ///     Annotates every Deployment with the number of its running pods: a GroupJoin of deployments and pods on the <c>app</c> label, re-checked
    ///     periodically by <c>Resync</c>, reconciled by <c>RunAsync</c>.
    /// </summary>
    public sealed class DeploymentReadinessController
    {
        public const string RunningPodsAnnotation = "samples.steeltoe.io/running-pods";
        public const string ReconciledByAnnotation = "samples.steeltoe.io/reconciled-by";

        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;

        public DeploymentReadinessController(IKubernetesInformerFactory informers, IKubernetes kubernetes)
        {
            _informers = informers;
            _kubernetes = kubernetes;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        /// <summary>Written into <see cref="ReconciledByAnnotation" />, so tests can tell which replica acted.</summary>
        public string Identity { get; set; } = Environment.MachineName;

        public TimeSpan ResyncPeriod { get; set; } = TimeSpan.FromSeconds(10);

        public sealed record Readiness(string Namespace, string Name, int RunningPods);

        public Task RunAsync(string ns, RunOptions options, CancellationToken cancellationToken)
        {
            var deployments = _informers.Get<V1Deployment>().Where(d => d.Metadata.NamespaceProperty == ns);
            var pods = _informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == ns);
            return deployments
                .GroupJoin(pods,
                    d => d.Spec.Selector.MatchLabels["app"],
                    p => p.Metadata.Labels["app"],
                    (d, ps) => new Readiness(d.Metadata.NamespaceProperty, d.Metadata.Name, ps.Count(p => p.Status?.Phase == "Running" && !p.IsBeingDeleted())))
                .Resync(ResyncPeriod)
                .RunAsync(async (e, ct) =>
                {
                    Stats.Increment("reconciles");
                    if (e.EventFlags.HasFlag(EventTypeFlags.Sync))
                        Stats.Increment("syncs");
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                        return;
                    var patch = new V1Patch(
                        $"{{\"metadata\":{{\"annotations\":{{\"{RunningPodsAnnotation}\":\"{e.Value.RunningPods}\",\"{ReconciledByAnnotation}\":\"{Identity}\"}}}}}}",
                        V1Patch.PatchType.MergePatch);
                    await _kubernetes.AppsV1.PatchNamespacedDeploymentAsync(patch, e.Value.Name, e.Value.Namespace, cancellationToken: ct).ConfigureAwait(false);
                    Stats.Increment("patches");
                }, options ?? new RunOptions { MaxConcurrency = 4, Name = nameof(DeploymentReadinessController) }, cancellationToken);
        }
    }
}
