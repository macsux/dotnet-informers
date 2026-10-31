using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Operator;

namespace Steeltoe.Informers.Samples.AotSmoke
{
    /// <summary>
    ///     Mirrors each ConfigMap's data into an owned Secret, labels the ConfigMap once mirrored, and removes the Secret through a finalizer:
    ///     the operator layer (owned children, expectations, finalizers, saves, events) in a NativeAOT app.
    /// </summary>
    public sealed class MirrorController : Controller<V1ConfigMap>
    {
        public const string Finalizer = "smoke.steeltoe.io/mirror";
        public const string MirroredLabel = "smoke.steeltoe.io/mirrored";

        public override void Configure(ControllerBuilder<V1ConfigMap> controller) => controller
            .Where(x => x.Metadata.Name == "cm")
            .TriggerOn(Trigger.AllButStatus, x => x.Data)
            .Owns<V1Secret>()
            .Finalizer(Finalizer, CleanupAsync);

        public override async Task<ReconcileResult> ReconcileAsync(ReconcileContext<V1ConfigMap> context, CancellationToken cancellationToken)
        {
            var map = context.Object.Value;
            var secret = context.Owned<V1Secret>().FirstOrDefault();
            if (secret == null)
            {
                await context.CreateOwnedAsync(new V1Secret { Metadata = new V1ObjectMeta { Name = $"{map.Metadata.Name}-mirror" }, StringData = map.Data?.ToDictionary(x => x.Key, x => x.Value) }, cancellationToken);
                context.Events.Normal("Mirrored", "Created the mirror secret");
            }

            (context.Object.Edit().Metadata.Labels ??= new Dictionary<string, string>())[MirroredLabel] = "true";
            await context.SaveAsync(cancellationToken);
            return ReconcileResult.Done;
        }

        private async Task<ReconcileResult> CleanupAsync(ReconcileContext<V1ConfigMap> context, CancellationToken cancellationToken)
        {
            foreach (var secret in context.Owned<V1Secret>().Where(x => !x.Value.IsBeingDeleted()))
                await context.DeleteOwnedAsync(secret, cancellationToken);
            return context.Owned<V1Secret>().Count == 0 ? ReconcileResult.Done : ReconcileResult.Requeue();
        }
    }
}
