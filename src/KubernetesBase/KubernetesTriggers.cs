using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Trigger presets for Kubernetes objects (see <c>TriggerOn</c>), usable on typed, untyped and metadata-only objects.</summary>
    public static class KubernetesTriggers
    {
        /// <summary>
        ///     <c>metadata.generation</c>: changes when the API server persists a change to the object's desired state (for most resources
        ///     everything but <c>status</c> and <c>metadata</c>).
        /// </summary>
        public static ITrigger<IKubernetesObject<V1ObjectMeta>> Generation { get; } = Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.generation", x => x.Metadata?.Generation);

        /// <summary>
        ///     Everything a controller usually acts on except <c>status</c>: generation, labels, annotations, finalizers and owner references.
        ///     The usual choice for a controller that writes its own status.
        /// </summary>
        public static ITrigger<IKubernetesObject<V1ObjectMeta>> AllButStatus { get; } = Trigger.Combine(
            Generation,
            Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.labels", x => x.Metadata?.Labels),
            Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.annotations", x => x.Metadata?.Annotations),
            Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.finalizers", x => x.Metadata?.Finalizers),
            Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.ownerReferences", x => x.Metadata?.OwnerReferences?.Select(r => $"{r.ApiVersion}/{r.Kind}/{r.Name}/{r.Uid}/{r.Controller}/{r.BlockOwnerDeletion}").ToList()));

        /// <summary>
        ///     <c>metadata.deletionTimestamp</c> being set: the start of a deletion, which a controller with finalizers must react to. Every
        ///     <c>TriggerOn</c> over Kubernetes objects includes it.
        /// </summary>
        public static ITrigger<IKubernetesObject<V1ObjectMeta>> Deletion { get; } = Trigger.On<IKubernetesObject<V1ObjectMeta>>("metadata.deletionTimestamp", x => x.Metadata?.DeletionTimestamp != null);
    }
}

namespace Steeltoe.Informers.InformersBase
{
    public static class KubernetesTriggerExtensions
    {
        extension(Trigger)
        {
            /// <inheritdoc cref="KubernetesTriggers.Generation" />
            public static ITrigger<IKubernetesObject<V1ObjectMeta>> Generation => KubernetesTriggers.Generation;

            /// <inheritdoc cref="KubernetesTriggers.AllButStatus" />
            public static ITrigger<IKubernetesObject<V1ObjectMeta>> AllButStatus => KubernetesTriggers.AllButStatus;
        }
    }
}

namespace System.Linq
{
    /// <summary>
    ///     <c>TriggerOn</c> for Kubernetes objects: like the general operator, plus a deletion starting (<c>deletionTimestamp</c> set) always
    ///     triggers, so finalizers run whatever the selection.
    /// </summary>
    public static class KubernetesInformable
    {
        /// <inheritdoc cref="Informable.TriggerOn{TKey,TResource}(IInformable{TKey,TResource},Expression{Func{TResource,object}}[])" />
        public static IInformable<string, TResource> TriggerOn<TResource>(this IInformable<string, TResource> source, params Expression<Func<TResource, object>>[] selectors)
            where TResource : class, IKubernetesObject<V1ObjectMeta> =>
            Informable.TriggerOn(source, Trigger.Combine<TResource>(KubernetesTriggers.Deletion, Trigger.On(selectors)));

        /// <inheritdoc cref="Informable.TriggerOn{TKey,TResource}(IInformable{TKey,TResource},ITrigger{TResource},Expression{Func{TResource,object}}[])" />
        public static IInformable<string, TResource> TriggerOn<TResource>(this IInformable<string, TResource> source, ITrigger<TResource> trigger, params Expression<Func<TResource, object>>[] selectors)
            where TResource : class, IKubernetesObject<V1ObjectMeta> =>
            Informable.TriggerOn(source, selectors == null || selectors.Length == 0
                ? Trigger.Combine<TResource>(KubernetesTriggers.Deletion, trigger)
                : Trigger.Combine<TResource>(KubernetesTriggers.Deletion, trigger, Trigger.On(selectors)));

        /// <inheritdoc cref="Informable.TriggerOn{TKey,TResource}(IInformable{TKey,TResource},ITrigger{TResource})" />
        public static IInformable<string, TResource> TriggerOn<TResource>(this IInformable<string, TResource> source, ITrigger<TResource> trigger)
            where TResource : class, IKubernetesObject<V1ObjectMeta> =>
            Informable.TriggerOn(source, Trigger.Combine<TResource>(KubernetesTriggers.Deletion, trigger));

        /// <inheritdoc cref="Informable.TriggerWhen{TKey,TResource}(IInformable{TKey,TResource},Func{TResource,TResource,bool})" />
        public static IInformable<string, TResource> TriggerWhen<TResource>(this IInformable<string, TResource> source, Func<TResource, TResource, bool> changed)
            where TResource : class, IKubernetesObject<V1ObjectMeta> =>
            Informable.TriggerOn(source, Trigger.Combine<TResource>(KubernetesTriggers.Deletion, Trigger.When(changed)));
    }
}
