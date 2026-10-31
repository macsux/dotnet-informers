using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Keys, owner references and lifecycle helpers for Kubernetes objects.</summary>
    public static class KubernetesObjectExtensions
    {
        /// <summary>
        ///     The informer key of an object: <c>namespace/name</c> for namespaced objects, <c>name</c> for cluster-scoped ones (client-go's
        ///     <c>MetaNamespaceKeyFunc</c>).
        /// </summary>
        public static string ObjectKey(this IMetadata<V1ObjectMeta> obj) => ObjectKey(obj?.Metadata?.NamespaceProperty, obj?.Metadata?.Name);

        /// <summary>The informer key for a namespace and name.</summary>
        public static string ObjectKey(string @namespace, string name) => string.IsNullOrEmpty(@namespace) ? name : $"{@namespace}/{name}";

        /// <summary>The UIDs of the object's owners, for joins on owner references (matching by UID, never by name).</summary>
        public static IEnumerable<string> OwnerUids(this IMetadata<V1ObjectMeta> obj) =>
            obj?.Metadata?.OwnerReferences?.Where(x => x.Uid != null).Select(x => x.Uid) ?? Enumerable.Empty<string>();

        /// <summary>The UIDs of the object's owners of the given kind (and API version, if given).</summary>
        public static IEnumerable<string> OwnerUids(this IMetadata<V1ObjectMeta> obj, string kind, string apiVersion = null) =>
            OwnerReferences(obj, kind, apiVersion).Select(x => x.Uid);

        /// <summary>
        ///     The informer keys (<c>namespace/name</c>) of the object's owners. Owners live in the object's namespace (or are cluster-scoped, if
        ///     the object is). Prefer <see cref="OwnerUids(IMetadata{V1ObjectMeta})" /> for joins: a recreated owner with the same name has a new UID.
        /// </summary>
        public static IEnumerable<string> OwnerKeys(this IMetadata<V1ObjectMeta> obj) =>
            OwnerReferences(obj, null, null).Select(x => ObjectKey(obj.Metadata.NamespaceProperty, x.Name));

        /// <summary>The informer keys of the object's owners of the given kind (and API version, if given).</summary>
        public static IEnumerable<string> OwnerKeys(this IMetadata<V1ObjectMeta> obj, string kind, string apiVersion = null) =>
            OwnerReferences(obj, kind, apiVersion).Select(x => ObjectKey(obj.Metadata.NamespaceProperty, x.Name));

        /// <summary>Whether <paramref name="owner" /> is among the object's owners, matched by UID.</summary>
        public static bool OwnedBy(this IMetadata<V1ObjectMeta> obj, IMetadata<V1ObjectMeta> owner) =>
            owner?.Metadata?.Uid != null && obj?.Metadata?.OwnerReferences?.Any(x => x.Uid == owner.Metadata.Uid) == true;

        /// <summary>
        ///     Whether deletion of the object has begun: <c>deletionTimestamp</c> is set and the API server waits for finalizers to be removed.
        ///     A deletion first arrives as a <c>Modify</c> with this set; the <c>Delete</c> follows once the last finalizer is gone.
        /// </summary>
        public static bool IsBeingDeleted(this IMetadata<V1ObjectMeta> obj) => obj?.Metadata?.DeletionTimestamp != null;

        /// <summary>
        ///     A deep copy (a serialization round-trip through <see cref="InformerJson" />). Objects handed out by an informer are shared by every consumer and must not be modified;
        ///     clone one before changing it, e.g. to build an update.
        /// </summary>
        public static T DeepClone<T>(this T obj) where T : class
        {
            if (obj == null)
                return null;
            if (obj is UntypedKubernetesObject untyped)
                return UntypedKubernetesObject.FromJson(untyped.Raw) as T;
            return InformerJson.Clone(obj);
        }

        /// <summary>
        ///     The object's <c>apiVersion</c> and <c>kind</c>; from its type's <see cref="KubernetesEntityAttribute" /> when unset (objects listed from
        ///     the API server often don't carry them).
        /// </summary>
        public static (string ApiVersion, string Kind) TypeMeta(this IKubernetesObject obj)
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            if (!string.IsNullOrEmpty(obj.ApiVersion) && !string.IsNullOrEmpty(obj.Kind))
                return (obj.ApiVersion, obj.Kind);
            var attribute = obj.GetType().GetCustomAttribute<KubernetesEntityAttribute>();
            if (attribute == null)
                return (obj.ApiVersion, obj.Kind);
            return (string.IsNullOrEmpty(obj.ApiVersion) ? (string.IsNullOrEmpty(attribute.Group) ? attribute.ApiVersion : $"{attribute.Group}/{attribute.ApiVersion}") : obj.ApiVersion,
                string.IsNullOrEmpty(obj.Kind) ? attribute.Kind : obj.Kind);
        }

        /// <summary>
        ///     An owner reference to <paramref name="owner" /> (apiVersion, kind, name, uid). <paramref name="controller" /> marks it as the managing
        ///     controller (at most one per object); <paramref name="blockOwnerDeletion" /> keeps a foreground deletion of the owner waiting for it.
        /// </summary>
        /// <exception cref="InvalidOperationException">The owner has no UID (it hasn't been created yet)</exception>
        public static V1OwnerReference ToOwnerReference(this IKubernetesObject<V1ObjectMeta> owner, bool controller = true, bool blockOwnerDeletion = true)
        {
            if (owner == null)
                throw new ArgumentNullException(nameof(owner));
            if (string.IsNullOrEmpty(owner.Metadata?.Uid))
                throw new InvalidOperationException($"{owner.Kind} {owner.Metadata?.Name} has no UID: only an object that exists can be an owner");
            var (apiVersion, kind) = owner.TypeMeta();
            if (string.IsNullOrEmpty(apiVersion) || string.IsNullOrEmpty(kind))
                throw new InvalidOperationException($"The apiVersion and kind of {owner.Metadata.Name} are unknown: set them or annotate its type with {nameof(KubernetesEntityAttribute)}");
            return new V1OwnerReference
            {
                ApiVersion = apiVersion,
                Kind = kind,
                Name = owner.Metadata.Name,
                Uid = owner.Metadata.Uid,
                Controller = controller,
                BlockOwnerDeletion = blockOwnerDeletion,
            };
        }

        /// <summary>
        ///     Adds an owner reference to <paramref name="owner" /> to the object (replacing an existing one to the same UID) and returns the object.
        ///     A namespaced object can only be owned by an object in its namespace or by a cluster-scoped one; when the object has no namespace
        ///     yet, it takes the owner's.
        /// </summary>
        /// <exception cref="InvalidOperationException">The owner has no UID, is in another namespace, or a second controller is set</exception>
        public static T WithOwnerReference<T>(this T obj, IKubernetesObject<V1ObjectMeta> owner, bool controller = true, bool blockOwnerDeletion = true)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            var reference = owner.ToOwnerReference(controller, blockOwnerDeletion);
            obj.Metadata ??= new V1ObjectMeta();
            var ownerNamespace = owner.Metadata.NamespaceProperty;
            if (!string.IsNullOrEmpty(ownerNamespace))
            {
                if (string.IsNullOrEmpty(obj.Metadata.NamespaceProperty))
                    obj.Metadata.NamespaceProperty = ownerNamespace;
                else if (obj.Metadata.NamespaceProperty != ownerNamespace)
                    throw new InvalidOperationException($"{obj.Metadata.Name} in namespace {obj.Metadata.NamespaceProperty} can't be owned by {owner.Metadata.Name} in namespace {ownerNamespace}");
            }

            var references = obj.Metadata.OwnerReferences ??= new List<V1OwnerReference>();
            for (var i = references.Count - 1; i >= 0; i--)
            {
                if (references[i].Uid == reference.Uid)
                    references.RemoveAt(i);
            }

            if (controller && references.Any(x => x.Controller == true))
                throw new InvalidOperationException($"{obj.Metadata.Name} already has a controller owner reference");
            references.Add(reference);
            return obj;
        }

        private static IEnumerable<V1OwnerReference> OwnerReferences(IMetadata<V1ObjectMeta> obj, string kind, string apiVersion) =>
            obj?.Metadata?.OwnerReferences?.Where(x => x.Uid != null && (kind == null || x.Kind == kind) && (apiVersion == null || x.ApiVersion == apiVersion))
            ?? Enumerable.Empty<V1OwnerReference>();
    }
}
