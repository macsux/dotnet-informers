using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>
    ///     A replica controller for the <see cref="AppSet" /> custom resource: keeps <c>spec.replicas</c> pods running, owned by the AppSet (matched
    ///     by UID), guards against duplicate creates with <see cref="Expectations" />, deletes its pods through a finalizer, writes status without
    ///     reacting to its own writes, and requeues while pods start.
    /// </summary>
    public sealed class AppSetController
    {
        private readonly IKubernetesInformerFactory _informers;
        private readonly IKubernetes _kubernetes;
        private readonly KubernetesGenericClient _generic;
        private readonly ILogger _logger;
        private readonly Expectations _expectations = new Expectations(TimeSpan.FromMinutes(1));

        public AppSetController(IKubernetesInformerFactory informers, IKubernetes kubernetes, KubernetesGenericClient generic, ILogger logger = null)
        {
            _informers = informers;
            _kubernetes = kubernetes;
            _generic = generic;
            _logger = logger ?? NullLogger.Instance;
        }

        public ControllerStats Stats { get; } = new ControllerStats();

        /// <summary>Test hook: runs right before a status write (e.g. to change the AppSet concurrently and force a 409).</summary>
        public Func<AppSet, Task> BeforeStatusWrite { get; set; }

        /// <summary>What the controller reconciles: an AppSet and its live pods (not being deleted).</summary>
        public sealed class AppSetState : IEquatable<AppSetState>
        {
            public AppSetState(AppSet set, IReadOnlyList<V1Pod> pods)
            {
                Set = set;
                Pods = pods;
            }

            public AppSet Set { get; }
            public IReadOnlyList<V1Pod> Pods { get; }

            /// <summary>
            ///     What the controller acts on, without the AppSet's status: status-only changes (including the controller's own writes) don't
            ///     trigger a reconcile.
            /// </summary>
            public string Signature =>
                $"{Set.Metadata.Generation}|{Set.IsBeingDeleted()}|{string.Join(",", Set.Metadata.Finalizers ?? new List<string>())}|" +
                string.Join(",", Pods.OrderBy(p => p.Metadata.Name).Select(p => $"{p.Metadata.Name}:{p.Status?.Phase}:{p.IsBeingDeleted()}"));

            public bool Equals(AppSetState other) =>
                other != null && Set.Metadata.ResourceVersion == other.Set.Metadata.ResourceVersion &&
                Pods.Select(x => x.Metadata.ResourceVersion).OrderBy(x => x).SequenceEqual(other.Pods.Select(x => x.Metadata.ResourceVersion).OrderBy(x => x));

            public override bool Equals(object obj) => Equals(obj as AppSetState);
            public override int GetHashCode() => Set.Metadata.ResourceVersion?.GetHashCode() ?? 0;
        }

        public Task RunAsync(string ns, CancellationToken cancellationToken)
        {
            var appSets = _informers.Get<AppSet>().Where(x => x.Metadata.NamespaceProperty == ns);
            // observe pod adds/deletes per owner before the join sees them, so expectations are met by the time a reconcile sees the pods
            var pods = _expectations.Observe(
                _informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == ns && p.GetLabel(AppSet.OwnerLabel) != null),
                p => p.OwnerKeys("AppSet"));
            return appSets
                .GroupJoin(pods, s => s.Metadata.Uid, p => p.OwnerUids("AppSet"), (s, ps) => new AppSetState(s, ps.ToList()))
                // after the join, so the join always holds the latest AppSet; status-only changes are dropped here
                .DistinctUntilChanged(x => x.Signature)
                .RunAsync(ReconcileAsync, new RunOptions { MaxConcurrency = 4, Name = nameof(AppSetController), Logger = _logger }, cancellationToken);
        }

        private async Task<ReconcileResult> ReconcileAsync(ResourceEvent<string, AppSetState> e, CancellationToken ct)
        {
            Stats.Increment("reconciles");
            if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
            {
                _expectations.Delete(e.Key);
                return ReconcileResult.Done;
            }

            var set = e.Value.Set;
            var live = e.Value.Pods.Where(p => !p.IsBeingDeleted()).ToList();
            if (set.IsBeingDeleted())
                return await FinalizeAsync(e.Key, set, e.Value.Pods, ct).ConfigureAwait(false);

            if (set.Metadata.Finalizers?.Contains(AppSet.Finalizer) != true)
                set = await UpdateFinalizersAsync(set, finalizers => finalizers.Append(AppSet.Finalizer), ct).ConfigureAwait(false);

            if (!_expectations.IsSatisfied(e.Key))
            {
                // our earlier creates/deletes aren't all observed yet: their events will trigger us again (or the timeout does)
                Stats.Increment("expectations-pending");
                Stats.Increment("requeue-after");
                return ReconcileResult.RequeueAfter(_expectations.RemainingTime(e.Key));
            }

            var missing = set.Spec.Replicas - live.Count;
            if (missing > 0)
            {
                _expectations.ExpectCreations(e.Key, missing);
                for (var i = 0; i < missing; i++)
                {
                    try
                    {
                        await _kubernetes.CoreV1.CreateNamespacedPodAsync(NewPod(set), set.Metadata.NamespaceProperty, cancellationToken: ct).ConfigureAwait(false);
                        Stats.Increment("creates");
                    }
                    catch
                    {
                        _expectations.CreationFailed(e.Key); // will never be observed
                        throw;
                    }
                }
            }
            else if (missing < 0)
            {
                var extra = live.OrderByDescending(p => p.Metadata.CreationTimestamp).ThenBy(p => p.Metadata.Name).Take(-missing).ToList();
                _expectations.ExpectDeletions(e.Key, extra.Count);
                foreach (var pod in extra)
                {
                    if (!await DeletePodAsync(pod, ct).ConfigureAwait(false))
                        _expectations.DeletionFailed(e.Key);
                }
            }

            var ready = live.Count(p => p.Status?.Phase == "Running");
            if (set.Status == null || set.Status.Replicas != live.Count || set.Status.ReadyReplicas != ready || set.Status.ObservedGeneration != set.Metadata.Generation)
            {
                // write against the current resourceVersion: the event's copy can be older than the last status write, because status-only
                // changes are filtered out before RunAsync (a retry after a conflict reads it again)
                var update = await ReadAsync(set, ct).ConfigureAwait(false);
                update.Status = new AppSetStatus { Replicas = live.Count, ReadyReplicas = ready, ObservedGeneration = set.Metadata.Generation };
                if (BeforeStatusWrite != null)
                    await BeforeStatusWrite(update).ConfigureAwait(false);
                await CountConflicts(() => _generic.ReplaceWithHttpMessagesAsync(update, statusOnly: true, cancellationToken: ct)).ConfigureAwait(false);
                Stats.Increment("status-writes");
            }

            if (missing == 0 && ready < set.Spec.Replicas)
            {
                // planned re-check while pods start (their phase changes trigger us too)
                Stats.Increment("requeue-after");
                return ReconcileResult.RequeueAfter(TimeSpan.FromSeconds(2));
            }

            return ReconcileResult.Done;
        }

        private async Task<ReconcileResult> FinalizeAsync(string key, AppSet set, IReadOnlyList<V1Pod> pods, CancellationToken ct)
        {
            if (set.Metadata.Finalizers?.Contains(AppSet.Finalizer) != true)
                return ReconcileResult.Done;
            if (pods.Count > 0)
            {
                foreach (var pod in pods.Where(p => !p.IsBeingDeleted()))
                    await DeletePodAsync(pod, ct).ConfigureAwait(false);
                // not done until the pods are gone: try again with backoff (their deletes trigger us too)
                Stats.Increment("requeue");
                return ReconcileResult.Requeue();
            }

            await UpdateFinalizersAsync(set, finalizers => finalizers.Where(x => x != AppSet.Finalizer), ct).ConfigureAwait(false);
            _expectations.Delete(key);
            Stats.Increment("finalized");
            return ReconcileResult.Done;
        }

        private async Task<AppSet> ReadAsync(AppSet set, CancellationToken ct) =>
            (await _generic.ReadWithHttpMessagesAsync<AppSet>(set.Metadata.Name, set.Metadata.NamespaceProperty, cancellationToken: ct).ConfigureAwait(false)).Body;

        /// <summary>Changes the finalizers of the current object, guarded by its resourceVersion (a merge patch replaces the whole list).</summary>
        private async Task<AppSet> UpdateFinalizersAsync(AppSet set, Func<IEnumerable<string>, IEnumerable<string>> change, CancellationToken ct)
        {
            var current = await ReadAsync(set, ct).ConfigureAwait(false);
            var finalizers = change(current.Metadata.Finalizers ?? new List<string>()).Distinct().ToList();
            var patch = new V1Patch(KubernetesJson.Serialize(new { metadata = new { finalizers, resourceVersion = current.Metadata.ResourceVersion } }), V1Patch.PatchType.MergePatch);
            var response = await CountConflicts(() => _generic.PatchWithHttpMessagesAsync<AppSet>(patch, set.Metadata.Name, set.Metadata.NamespaceProperty, cancellationToken: ct)).ConfigureAwait(false);
            return response.Body;
        }

        private async Task<bool> DeletePodAsync(V1Pod pod, CancellationToken ct)
        {
            try
            {
                await _kubernetes.CoreV1.DeleteNamespacedPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, gracePeriodSeconds: 0, cancellationToken: ct).ConfigureAwait(false);
                Stats.Increment("deletes");
                return true;
            }
            catch (HttpOperationException ex) when (ex.Response.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        private async Task<T> CountConflicts<T>(Func<Task<T>> write)
        {
            try
            {
                return await write().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex.IsConflict())
            {
                Stats.Increment("conflicts");
                throw; // RunAsync retries promptly with the latest state
            }
        }

        private static V1Pod NewPod(AppSet set) => new V1Pod
        {
            Metadata = new V1ObjectMeta
            {
                GenerateName = $"{set.Metadata.Name}-",
                NamespaceProperty = set.Metadata.NamespaceProperty,
                Labels = new Dictionary<string, string> { [AppSet.OwnerLabel] = set.Metadata.Name },
                OwnerReferences = new List<V1OwnerReference>
                {
                    new V1OwnerReference { ApiVersion = set.ApiVersion, Kind = set.Kind, Name = set.Metadata.Name, Uid = set.Metadata.Uid, Controller = true, BlockOwnerDeletion = true },
                },
            },
            Spec = new V1PodSpec
            {
                TerminationGracePeriodSeconds = 0,
                Containers = new List<V1Container> { new V1Container { Name = "app", Image = set.Spec.Image ?? "registry.k8s.io/pause:3.10" } },
            },
        };
    }
}
