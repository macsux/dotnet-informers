using System.Collections.Generic;
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
    ///     Annotates every Secret with the pods that reference it (volumes, <c>envFrom</c>, <c>env.valueFrom</c>): a GroupJoin of metadata-only
    ///     Secrets (their data is never loaded) with pods on a multi-valued key (one pod references several secrets).
    /// </summary>
    public sealed class SecretUsageController
    {
        public const string UsedByAnnotation = "samples.steeltoe.io/used-by";

        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;

        public SecretUsageController(IKubernetesInformerFactory informers, IKubernetes kubernetes)
        {
            _informers = informers;
            _kubernetes = kubernetes;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        public static IEnumerable<string> ReferencedSecrets(V1Pod pod)
        {
            var spec = pod.Spec;
            var fromVolumes = spec.Volumes?.Select(v => v.Secret?.SecretName) ?? Enumerable.Empty<string>();
            var containers = (spec.Containers ?? new List<V1Container>()).Concat(spec.InitContainers ?? new List<V1Container>()).ToList();
            var fromEnvFrom = containers.SelectMany(c => c.EnvFrom ?? new List<V1EnvFromSource>()).Select(x => x.SecretRef?.Name);
            var fromEnv = containers.SelectMany(c => c.Env ?? new List<V1EnvVar>()).Select(x => x.ValueFrom?.SecretKeyRef?.Name);
            return fromVolumes.Concat(fromEnvFrom).Concat(fromEnv).Where(x => x != null).Distinct();
        }

        public Task RunAsync(string ns, CancellationToken cancellationToken)
        {
            var secrets = _informers.MetadataOnly<V1Secret>().Where(s => s.Metadata.NamespaceProperty == ns);
            var pods = _informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == ns);
            return secrets
                .GroupJoin(pods, s => s.Metadata.Name, ReferencedSecrets, (s, ps) => (Secret: s, UsedBy: string.Join(",", ps.Select(p => p.Metadata.Name).OrderBy(x => x))))
                .RunAsync(async (e, ct) =>
                {
                    Stats.Increment("reconciles");
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete) || e.Value.Secret.GetAnnotation(UsedByAnnotation) == e.Value.UsedBy)
                        return;
                    var patch = new V1Patch(KubernetesJson.Serialize(new { metadata = new { annotations = new Dictionary<string, string> { [UsedByAnnotation] = e.Value.UsedBy } } }), V1Patch.PatchType.MergePatch);
                    await _kubernetes.CoreV1.PatchNamespacedSecretAsync(patch, e.Value.Secret.Metadata.Name, ns, cancellationToken: ct).ConfigureAwait(false);
                    Stats.Increment("patches");
                }, new RunOptions { MaxConcurrency = 2, Name = nameof(SecretUsageController) }, cancellationToken);
        }
    }
}
