using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>What informers need from the API server: list/watch by group/version/resource, and scope discovery.</summary>
    internal interface IKubernetesResourceClient
    {
        /// <summary>
        ///     Sends a list or watch request for a resource addressed by group/version/resource and returns the response once its headers
        ///     arrived, so a watch body can be streamed. Throws <see cref="HttpOperationException" /> for an unsuccessful status.
        /// </summary>
        Task<HttpResponseMessage> SendListAsync(GroupVersionResource resource, string namespaceParameter, KubernetesListRequest request, CancellationToken cancellationToken = default);

        /// <summary>Whether a resource is namespaced, from API discovery (cached).</summary>
        Task<bool> IsNamespacedAsync(GroupVersionResource resource, CancellationToken cancellationToken = default);
    }

    /// <summary>
    ///     A typed client for any resource: reads and writes go through the configured JSON metadata (<see cref="InformersBase.InformerJson" />),
    ///     so it is trim- and AOT-safe. The resource of an object comes from its type's <see cref="KubernetesEntityAttribute" />, or, for
    ///     <see cref="UntypedKubernetesObject" /> and <see cref="PartialObjectMetadata" />, from its <c>apiVersion</c> and <c>kind</c> through
    ///     discovery. Unsuccessful responses throw <see cref="HttpOperationException" /> (see <see cref="Extensions.IsNotFound" />,
    ///     <see cref="Extensions.IsConflict" />, <see cref="Extensions.IsAlreadyExists" />).
    /// </summary>
    public interface IKubernetesGenericClient
    {
        /// <summary>Reads an object; <see langword="null" /> when it doesn't exist.</summary>
        Task<T> GetAsync<T>(string name, string @namespace = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>Lists objects (every page), optionally filtered by label and field selectors.</summary>
        Task<IReadOnlyList<T>> ListAsync<T>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>Creates an object and returns it as stored.</summary>
        Task<T> CreateAsync<T>(T obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>Replaces an object (or its <c>/status</c> subresource), guarded by the <c>resourceVersion</c> it carries.</summary>
        Task<T> ReplaceAsync<T>(T obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>Sends a patch to the object's resource (or its <c>/status</c> subresource) and returns the patched object.</summary>
        Task<T> PatchAsync<T>(T obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>
        ///     Declares the full desired state of an object with server-side apply, owned by <paramref name="fieldManager" />: creates it or
        ///     updates the fields this manager owns.
        /// </summary>
        Task<T> ApplyAsync<T>(T desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>
        ///     Deletes an object. When the object carries a UID it's sent as a precondition, so a recreated object of the same name isn't deleted.
        ///     Returns false when it was already gone.
        /// </summary>
        Task<bool> DeleteAsync<T>(T obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <summary>The resource an object is served from, with its scope and subresources (from discovery, cached).</summary>
        Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default);

        /// <summary>Scope and subresources of a resource (from discovery, cached).</summary>
        Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default);
    }

    /// <summary>Options of a write request.</summary>
    public sealed class KubernetesWriteOptions
    {
        /// <summary>The field manager recorded for the write.</summary>
        public string FieldManager { get; set; }

        /// <summary>Validate and return the result without persisting it.</summary>
        public bool DryRun { get; set; }
    }

    /// <summary>A resource as discovery describes it.</summary>
    public sealed record KubernetesResourceInfo(GroupVersionResource Resource, string Kind, bool Namespaced, bool HasStatus, bool HasScale);
}
