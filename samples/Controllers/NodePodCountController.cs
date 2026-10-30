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
    ///     Annotates every Node (cluster-scoped) with how many running pods of one namespace it hosts: a GroupJoin of nodes with pods on
    ///     <c>spec.nodeName</c>. The namespace and the <c>status.phase</c> filter are pushed down to the API server (field selector).
    /// </summary>
    public sealed class NodePodCountController
    {
        public static string Annotation(string ns) => $"samples.steeltoe.io/running-in-{ns}";

        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;

        public NodePodCountController(IKubernetesInformerFactory informers, IKubernetes kubernetes)
        {
            _informers = informers;
            _kubernetes = kubernetes;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        public Task RunAsync(string ns, CancellationToken cancellationToken) =>
            _informers.Get<V1Node>()
                .GroupJoin(_informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == ns && p.Status.Phase == "Running"),
                    n => n.Metadata.Name,
                    p => p.Spec.NodeName,
                    (n, ps) => (Node: n.Metadata.Name, Running: ps.Count()))
                .RunAsync(async (e, ct) =>
                {
                    Stats.Increment("reconciles");
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                        return;
                    var patch = new V1Patch($"{{\"metadata\":{{\"annotations\":{{\"{Annotation(ns)}\":\"{e.Value.Running}\"}}}}}}", V1Patch.PatchType.MergePatch);
                    await _kubernetes.CoreV1.PatchNodeAsync(patch, e.Value.Node, cancellationToken: ct).ConfigureAwait(false);
                }, new RunOptions { Name = nameof(NodePodCountController) }, cancellationToken);
    }
}
