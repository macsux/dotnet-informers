using System;
using System.Collections.Generic;
using System.Linq;
using k8s;
using k8s.Models;

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
        ///     A deep copy (a serialization round-trip). Objects handed out by an informer are shared by every consumer and must not be modified;
        ///     clone one before changing it, e.g. to build an update.
        /// </summary>
        public static T DeepClone<T>(this T obj) where T : class
        {
            if (obj == null)
                return null;
            if (obj is UntypedKubernetesObject untyped)
                return UntypedKubernetesObject.FromJson(untyped.Raw) as T;
            return KubernetesJson.Deserialize<T>(KubernetesJson.Serialize(obj));
        }

        private static IEnumerable<V1OwnerReference> OwnerReferences(IMetadata<V1ObjectMeta> obj, string kind, string apiVersion) =>
            obj?.Metadata?.OwnerReferences?.Where(x => x.Uid != null && (kind == null || x.Kind == kind) && (apiVersion == null || x.ApiVersion == apiVersion))
            ?? Enumerable.Empty<V1OwnerReference>();
    }
}
