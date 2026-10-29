using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using k8s.Autorest;

namespace Steeltoe.Informers.KubernetesBase
{
    internal interface IKubernetesGenericClient
    {
        Task<HttpOperationResponse<KubernetesList<T>>> ListWithHttpMessagesAsync<T>(
            string namespaceParameter = default,
            bool? allowWatchBookmarks = default,
            string continueParameter = default,
            string fieldSelector = default,
            string labelSelector = default,
            int? limit = default,
            string resourceVersion = default,
            TimeSpan? timeout = default,
            bool? watch = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse> ListWithHttpMessagesAsync(
            Type type,
            string namespaceParameter = default,
            bool? allowWatchBookmarks = default,
            string continueParameter = default,
            string fieldSelector = default,
            string labelSelector = default,
            int? limit = default,
            string resourceVersion = default,
            TimeSpan? timeout = default,
            bool? watch = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<T>> ReadWithHttpMessagesAsync<T>(
            string name,
            string namespaceParameter = default,
            bool? isExact = default,
            bool? isExport = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = default,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse> ReadWithHttpMessagesAsync(
            Type type,
            string name,
            string namespaceParameter = default,
            bool? isExact = default,
            bool? isExport = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<T>> CreateWithHttpMessagesAsync<T>(
            T body,
            string namespaceParameter = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = default,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse> CreateWithHttpMessagesAsync(
            object body,
            string namespaceParameter = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<V1Status>> DeleteWithHttpMessagesAsync<TResource>(
            TResource resource,
            V1DeleteOptions body = default,
            DryRun? dryRun = default,
            TimeSpan? gracePeriod = default,
            bool? orphanDependents = default,
            PropagationPolicy? propagationPolicy = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default)
            where TResource : IKubernetesObject<V1ObjectMeta>;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<V1Status>> DeleteWithHttpMessagesAsync<TResource>(
            string name = default,
            string namespaceParameter = default,
            V1DeleteOptions body = default,
            DryRun? dryRun = default,
            TimeSpan? gracePeriod = default,
            bool? orphanDependents = default,
            PropagationPolicy? propagationPolicy = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default)
            where TResource : IKubernetesObject;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<V1Status>> DeleteWithHttpMessagesAsync(
            Type type,
            string name = default,
            string namespaceParameter = default,
            V1DeleteOptions body = default,
            DryRun? dryRun = default,
            TimeSpan? gracePeriod = default,
            bool? orphanDependents = default,
            PropagationPolicy? propagationPolicy = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<T>> PatchWithHttpMessagesAsync<T>(
            V1Patch body,
            string name,
            string namespaceParameter = default,
            bool? statusOnly = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? force = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse> PatchWithHttpMessagesAsync(
            Type type,
            V1Patch body,
            string name,
            string namespaceParameter = default,
            bool? statusOnly = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? force = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<T>> ReplaceWithHttpMessagesAsync<T>(
            T body,
            bool? statusOnly = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>;

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse<T>> ReplaceWithHttpMessagesAsync<T>(
            T body,
            string name,
            string namespaceParameter = default,
            bool? statusOnly = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);

        /// <inheritdoc cref="IKubernetes" />
        Task<HttpOperationResponse> ReplaceWithHttpMessagesAsync(
            object body,
            string name,
            string namespaceParameter = default,
            bool? statusOnly = default,
            DryRun? dryRun = default,
            string fieldManager = default,
            bool? isPretty = default,
            Dictionary<string, List<string>> customHeaders = null,
            CancellationToken cancellationToken = default);
    }
}