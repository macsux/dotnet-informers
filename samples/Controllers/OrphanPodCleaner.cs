using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>
    ///     Deletes pods (label <see cref="CheckLabel" />) whose <see cref="OwnerAnnotation" /> names a ReplicaSet that doesn't exist. Both are
    ///     metadata-only informers. The handler consults a ReplicaSet cache, so it must not run before that cache holds the complete list
    ///     (otherwise every pod looks orphaned): it waits for the informer's and the cache's sync first.
    /// </summary>
    public sealed class OrphanPodCleaner
    {
        public const string CheckLabel = "samples.steeltoe.io/orphan-check";
        public const string OwnerAnnotation = "samples.steeltoe.io/owner-replicaset";

        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;

        public OrphanPodCleaner(IKubernetesInformerFactory informers, IKubernetes kubernetes)
        {
            _informers = informers;
            _kubernetes = kubernetes;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        public async Task RunAsync(string ns, CancellationToken cancellationToken)
        {
            var replicaSets = _informers.MetadataOnly<V1ReplicaSet>().Where(x => x.Metadata.NamespaceProperty == ns);
            using var replicaSetCache = new SimpleCache<string, PartialObjectMetadata>(replicaSets, cancellationToken);
            await replicaSets.WaitForSyncAsync(cancellationToken).ConfigureAwait(false);
            await replicaSetCache.Synchronized.ConfigureAwait(false);
            Stats.Increment("synced");

            await _informers.MetadataOnly<V1Pod>()
                .Where(p => p.Metadata.NamespaceProperty == ns && p.GetLabel(CheckLabel) != null)
                .RunAsync(async (e, ct) =>
                {
                    Stats.Increment("reconciles");
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete) || e.Value.IsBeingDeleted())
                        return;
                    var owner = e.Value.GetAnnotation(OwnerAnnotation);
                    if (owner == null || replicaSetCache.ContainsKey(KubernetesObjectExtensions.ObjectKey(ns, owner)))
                        return;
                    try
                    {
                        await _kubernetes.CoreV1.DeleteNamespacedPodAsync(e.Value.Metadata.Name, ns, gracePeriodSeconds: 0, cancellationToken: ct).ConfigureAwait(false);
                        Stats.Increment("deleted");
                    }
                    catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.NotFound)
                    {
                    }
                }, new RunOptions { Name = nameof(OrphanPodCleaner) }, cancellationToken).ConfigureAwait(false);
        }
    }
}
