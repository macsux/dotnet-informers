using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Operator;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>
    ///     A replica controller for the <see cref="AppSet" /> custom resource: keeps <c>spec.replicas</c> pods running, owned by the AppSet, deletes
    ///     them through a finalizer, writes status without reacting to its own writes, and re-checks while pods start. Owner references,
    ///     expectations, finalizer bookkeeping and the status patch come from the operator layer.
    /// </summary>
    public sealed class AppSetController(ControllerStats stats, AppSetTestHooks hooks = null) : Controller<AppSet>
    {
        public override void Configure(ControllerBuilder<AppSet> controller) => controller
            .TriggerOn(Trigger.AllButStatus) // status-only changes, including our own status writes, don't start a reconcile
            .Owns<V1Pod>(p => new { p.Status.Phase, Deleting = p.Metadata.DeletionTimestamp != null })
            .Finalizer(AppSet.Finalizer, CleanupPodsAsync)
            .MaxConcurrency(4);

        public override async Task<ReconcileResult> ReconcileAsync(ReconcileContext<AppSet> context, CancellationToken cancellationToken)
        {
            stats.Increment("reconciles");
            var set = context.Object.Value;
            var live = context.Owned<V1Pod>().Where(p => !p.Value.IsBeingDeleted()).ToList();
            var missing = set.Spec.Replicas - live.Count;
            for (var i = 0; i < missing; i++)
            {
                await context.CreateOwnedAsync(NewPod(set), cancellationToken);
                stats.Increment("creates");
            }

            foreach (var extra in live.OrderByDescending(p => p.Value.Metadata.CreationTimestamp).ThenBy(p => p.Value.Metadata.Name).Take(-missing))
            {
                if (await context.DeleteOwnedAsync(extra, cancellationToken))
                    stats.Increment("deletes");
            }

            var ready = live.Count(p => p.Value.Status?.Phase == "Running");
            context.Object.Edit().Status = new AppSetStatus { Replicas = live.Count, ReadyReplicas = ready, ObservedGeneration = set.Metadata.Generation };
            if (context.Object.HasChanges())
                await SaveStatusAsync(context, cancellationToken);

            if (missing == 0 && ready < set.Spec.Replicas)
            {
                // planned re-check while pods start (their phase changes trigger us too)
                stats.Increment("requeue-after");
                return ReconcileResult.RequeueAfter(TimeSpan.FromSeconds(2));
            }

            return ReconcileResult.Done;
        }

        private async Task<ReconcileResult> CleanupPodsAsync(ReconcileContext<AppSet> context, CancellationToken cancellationToken)
        {
            var pods = context.Owned<V1Pod>();
            if (pods.Count == 0)
            {
                stats.Increment("finalized");
                return ReconcileResult.Done; // the finalizer goes, and the deletion completes
            }

            foreach (var pod in pods.Where(p => !p.Value.IsBeingDeleted()))
                await context.DeleteOwnedAsync(pod, cancellationToken);
            // not done until the pods are gone: try again with backoff (their deletes trigger us too)
            stats.Increment("requeue");
            return ReconcileResult.Requeue();
        }

        private async Task SaveStatusAsync(ReconcileContext<AppSet> context, CancellationToken cancellationToken)
        {
            if (hooks?.BeforeStatusWrite != null)
                await hooks.BeforeStatusWrite(context.Object.Original);
            try
            {
                await context.SaveAsync(cancellationToken); // PATCH .../status with only what changed, guarded by resourceVersion
                stats.Increment("status-writes");
            }
            catch (Exception ex) when (ex.IsConflict())
            {
                stats.Increment("conflicts");
                throw; // retried promptly with the latest state
            }
        }

        private static V1Pod NewPod(AppSet set) => new V1Pod
        {
            Metadata = new V1ObjectMeta { GenerateName = $"{set.Metadata.Name}-", Labels = new Dictionary<string, string> { [AppSet.OwnerLabel] = set.Metadata.Name } },
            Spec = new V1PodSpec
            {
                TerminationGracePeriodSeconds = 0,
                Containers = new List<V1Container> { new V1Container { Name = "app", Image = set.Spec.Image ?? "registry.k8s.io/pause:3.10" } },
            },
        };
    }

    /// <summary>Test hook: runs right before a status write (e.g. to change the AppSet concurrently and force a 409).</summary>
    public sealed class AppSetTestHooks
    {
        public Func<AppSet, Task> BeforeStatusWrite { get; set; }
    }
}
