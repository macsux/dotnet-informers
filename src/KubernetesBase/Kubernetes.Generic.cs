using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Authentication;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <inheritdoc cref="IKubernetesGenericClient" />
    public class KubernetesGenericClient : IKubernetesGenericClient, IKubernetesResourceClient
    {
        private const string JsonContentType = "application/json";
        private readonly HttpClient _client;
        private readonly Uri _baseUri;
        private readonly ServiceClientCredentials _credentials;
        private readonly ConcurrentDictionary<string, Task<V1APIResourceList>> _discovery = new ConcurrentDictionary<string, Task<V1APIResourceList>>();

        /// <summary>
        ///     Creates a generic client that reuses the HTTP pipeline, base address and credentials of an existing <see cref="k8s.Kubernetes" /> client
        /// </summary>
        public KubernetesGenericClient(k8s.Kubernetes kubernetes) : this(kubernetes.HttpClient, kubernetes.BaseUri, kubernetes.Credentials)
        {
        }

        public KubernetesGenericClient(HttpClient client, Uri baseUri, ServiceClientCredentials credentials = null)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _baseUri = baseUri;
            _credentials = credentials;
        }

        /// <inheritdoc />
        public async Task<T> GetAsync<T>(string name, string @namespace = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
            try
            {
                using var response = await SendAsync(HttpMethod.Get, BuildUrl(GroupVersionResource.For<T>(), @namespace, name), null, null, cancellationToken).ConfigureAwait(false);
                return await ReadAsync<T>(response, cancellationToken).ConfigureAwait(false);
            }
            catch (HttpOperationException ex) when (ex.IsNotFound())
            {
                return default;
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyList<T>> ListAsync<T>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            var info = InformerJson.GetTypeInfo<T>();
            var items = new List<T>();
            string continueToken = null;
            do
            {
                using var response = await ((IKubernetesResourceClient)this).SendListAsync(GroupVersionResource.For<T>(), @namespace, new KubernetesListRequest
                {
                    LabelSelector = labelSelector,
                    FieldSelector = fieldSelector,
                    Limit = 500,
                    Continue = continueToken,
                }, cancellationToken).ConfigureAwait(false);
                await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (document.RootElement.TryGetProperty("items", out var list) && list.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in list.EnumerateArray())
                        items.Add(item.Deserialize(info));
                }

                continueToken = document.RootElement.TryGetProperty("metadata", out var metadata) && metadata.TryGetProperty("continue", out var token) ? token.GetString() : null;
            }
            while (!string.IsNullOrEmpty(continueToken));

            return items;
        }

        /// <inheritdoc />
        public async Task<T> CreateAsync<T>(T obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            var resource = await GetResourceInfoAsync(obj, cancellationToken).ConfigureAwait(false);
            var url = BuildUrl(resource.Resource, resource.Namespaced ? obj.Metadata?.NamespaceProperty : null) + Query(options);
            using var response = await SendAsync(HttpMethod.Post, url, SerializeWithTypeMeta(obj), JsonContentType, cancellationToken).ConfigureAwait(false);
            return await ReadAsync<T>(response, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<T> ReplaceAsync<T>(T obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            var url = await ObjectUrlAsync(obj, status, cancellationToken).ConfigureAwait(false) + Query(options);
            using var response = await SendAsync(HttpMethod.Put, url, SerializeWithTypeMeta(obj), JsonContentType, cancellationToken).ConfigureAwait(false);
            return await ReadAsync<T>(response, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<T> PatchAsync<T>(T obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            if (patch == null)
                throw new ArgumentNullException(nameof(patch));
            var url = await ObjectUrlAsync(obj, status, cancellationToken).ConfigureAwait(false) + Query(options);
            var content = patch.Content as string ?? InformerJson.Serialize(patch.Content);
            using var response = await SendAsync(HttpMethod.Patch, url, content, PatchContentType(patch.Type), cancellationToken).ConfigureAwait(false);
            return await ReadAsync<T>(response, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<T> ApplyAsync<T>(T desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (desired == null)
                throw new ArgumentNullException(nameof(desired));
            if (string.IsNullOrEmpty(fieldManager))
                throw new ArgumentNullException(nameof(fieldManager));
            var url = await ObjectUrlAsync(desired, false, cancellationToken).ConfigureAwait(false) + Query(new KubernetesWriteOptions { FieldManager = fieldManager }, force);
            // JSON is YAML: the apply content type accepts it
            using var response = await SendAsync(HttpMethod.Patch, url, SerializeWithTypeMeta(desired), PatchContentType(V1Patch.PatchType.ApplyPatch), cancellationToken).ConfigureAwait(false);
            return await ReadAsync<T>(response, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<bool> DeleteAsync<T>(T obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            options ??= new V1DeleteOptions();
            if (options.Preconditions == null && obj.Metadata?.Uid != null)
                options.Preconditions = new V1Preconditions { Uid = obj.Metadata.Uid };
            var url = await ObjectUrlAsync(obj, false, cancellationToken).ConfigureAwait(false);
            try
            {
                using var response = await SendAsync(HttpMethod.Delete, url, InformerJson.Serialize(options), JsonContentType, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (HttpOperationException ex) when (ex.IsNotFound())
            {
                return false;
            }
        }

        /// <inheritdoc />
        public async Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default)
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            var type = obj.GetType();
            if (type.GetCustomAttribute<KubernetesEntityAttribute>() != null && type != typeof(PartialObjectMetadata))
                return await GetResourceInfoAsync(GroupVersionResource.For(type), cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrEmpty(obj.ApiVersion) || string.IsNullOrEmpty(obj.Kind))
                throw new InvalidOperationException($"{type.Name} {obj.Metadata?.Name} has no apiVersion and kind to find its resource");
            var list = await DiscoverAsync(obj.ApiVersion, cancellationToken).ConfigureAwait(false);
            var match = list.Resources?.FirstOrDefault(x => x.Kind == obj.Kind && !x.Name.Contains('/'))
                ?? throw new InvalidOperationException($"The API server doesn't serve kind {obj.Kind} in {obj.ApiVersion}");
            var slash = obj.ApiVersion.IndexOf('/');
            var resource = slash < 0 ? new GroupVersionResource(string.Empty, obj.ApiVersion, match.Name) : new GroupVersionResource(obj.ApiVersion[..slash], obj.ApiVersion[(slash + 1)..], match.Name);
            return Describe(resource, list);
        }

        /// <inheritdoc />
        public async Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));
            return Describe(resource, await DiscoverAsync(resource.ApiVersion, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        async Task<bool> IKubernetesResourceClient.IsNamespacedAsync(GroupVersionResource resource, CancellationToken cancellationToken) =>
            (await GetResourceInfoAsync(resource, cancellationToken).ConfigureAwait(false)).Namespaced;

        /// <inheritdoc />
        async Task<HttpResponseMessage> IKubernetesResourceClient.SendListAsync(GroupVersionResource resource, string namespaceParameter, KubernetesListRequest request, CancellationToken cancellationToken)
        {
            if (resource == null)
                throw new ArgumentNullException(nameof(resource));
            request ??= new KubernetesListRequest();
            var sb = new StringBuilder();
            AddQueryParameter(sb, "allowWatchBookmarks", request.AllowWatchBookmarks?.ToString().ToLowerInvariant());
            AddQueryParameter(sb, "continue", request.Continue);
            AddQueryParameter(sb, "fieldSelector", request.FieldSelector);
            AddQueryParameter(sb, "labelSelector", request.LabelSelector);
            AddQueryParameter(sb, "limit", request.Limit?.ToString());
            AddQueryParameter(sb, "resourceVersion", request.ResourceVersion);
            AddQueryParameter(sb, "timeoutSeconds", request.TimeoutSeconds?.ToString());
            AddQueryParameter(sb, "watch", request.Watch ? "true" : null);
            var headers = string.IsNullOrEmpty(request.Accept) ? null : request.Accept;
            return await SendAsync(HttpMethod.Get, BuildUrl(resource, namespaceParameter) + sb, null, null, cancellationToken, headers).ConfigureAwait(false);
        }

        private static KubernetesResourceInfo Describe(GroupVersionResource resource, V1APIResourceList list)
        {
            var match = list.Resources?.FirstOrDefault(x => x.Name == resource.Resource)
                ?? throw new InvalidOperationException($"The API server doesn't serve {resource}");
            var subresources = list.Resources.Where(x => x.Name.StartsWith(resource.Resource + "/", StringComparison.Ordinal)).Select(x => x.Name[(resource.Resource.Length + 1)..]).ToList();
            return new KubernetesResourceInfo(resource, match.Kind, match.Namespaced, subresources.Contains("status"), subresources.Contains("scale"));
        }

        private Task<V1APIResourceList> DiscoverAsync(string apiVersion, CancellationToken cancellationToken) =>
            _discovery.GetOrAdd(apiVersion, FetchDiscoveryAsync).WaitAsync(cancellationToken);

        private async Task<V1APIResourceList> FetchDiscoveryAsync(string apiVersion)
        {
            var url = $"{Root}/{(apiVersion.Contains('/') ? "apis" : "api")}/{apiVersion}";
            try
            {
                using var response = await SendAsync(HttpMethod.Get, url, null, null, CancellationToken.None).ConfigureAwait(false);
                return await ReadAsync<V1APIResourceList>(response, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                _discovery.TryRemove(apiVersion, out _); // don't cache failures
                throw;
            }
        }

        private async Task<string> ObjectUrlAsync(IKubernetesObject<V1ObjectMeta> obj, bool status, CancellationToken cancellationToken)
        {
            var name = obj.Metadata?.Name;
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("The object has no name", nameof(obj));
            var resource = await GetResourceInfoAsync(obj, cancellationToken).ConfigureAwait(false);
            return BuildUrl(resource.Resource, resource.Namespaced ? obj.Metadata.NamespaceProperty : null, name, status);
        }

        /// <summary>The object's JSON with <c>apiVersion</c> and <c>kind</c> filled in from its type when unset (without changing the object).</summary>
        private static string SerializeWithTypeMeta(IKubernetesObject<V1ObjectMeta> obj)
        {
            if (!string.IsNullOrEmpty(obj.ApiVersion) && !string.IsNullOrEmpty(obj.Kind))
                return InformerJson.Serialize(obj);
            var json = Patching.ToJson(obj);
            var (apiVersion, kind) = obj.TypeMeta();
            json["apiVersion"] ??= apiVersion;
            json["kind"] ??= kind;
            return json.ToJsonString();
        }

        private static async Task<T> ReadAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            var content = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            if (content.Length == 0)
                return default;
            try
            {
                return InformerJson.Deserialize<T>(content);
            }
            catch (JsonException ex)
            {
                throw new HttpOperationException("Unable to deserialize the response.", ex);
            }
        }

        private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, string body, string contentType, CancellationToken cancellationToken, string accept = null)
        {
            using var request = new HttpRequestMessage(method, new Uri(url));
            if (body != null)
            {
                request.Content = new StringContent(body, Encoding.UTF8);
                request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse($"{contentType}; charset=utf-8");
            }

            if (accept != null)
                request.Headers.TryAddWithoutValidation("Accept", accept);
            if (_credentials != null)
                await _credentials.ProcessHttpRequestAsync(request, cancellationToken).ConfigureAwait(false);

            // headers-only so watch responses can be streamed instead of buffered
            var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
                return response;
            var content = response.Content != null ? await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false) : string.Empty;
            var ex = new HttpOperationException($"Operation returned an invalid status code '{response.StatusCode}'")
            {
                Request = new HttpRequestMessageWrapper(request, body),
                Response = new HttpResponseMessageWrapper(response, content),
            };
            response.Dispose();
            throw ex;
        }

        private string Root => (_baseUri ?? _client.BaseAddress).AbsoluteUri.TrimEnd('/');

        private string BuildUrl(GroupVersionResource resource, string namespaceParameter, string name = null, bool statusOnly = false)
        {
            var sb = new StringBuilder(Root);
            sb.Append(string.IsNullOrEmpty(resource.Group) ? "/api/" : $"/apis/{resource.Group}/").Append(resource.Version);
            if (!string.IsNullOrEmpty(namespaceParameter))
                sb.Append("/namespaces/").Append(Uri.EscapeDataString(namespaceParameter));
            sb.Append('/').Append(resource.Resource);
            if (!string.IsNullOrEmpty(name))
                sb.Append('/').Append(Uri.EscapeDataString(name));
            if (statusOnly)
                sb.Append("/status");
            return sb.ToString();
        }

        private static string Query(KubernetesWriteOptions options, bool force = false)
        {
            var sb = new StringBuilder();
            AddQueryParameter(sb, "fieldManager", options?.FieldManager);
            AddQueryParameter(sb, "dryRun", options?.DryRun == true ? "All" : null);
            AddQueryParameter(sb, "force", force ? "true" : null);
            return sb.ToString();
        }

        private static string PatchContentType(V1Patch.PatchType type) => type switch
        {
            V1Patch.PatchType.MergePatch => "application/merge-patch+json",
            V1Patch.PatchType.StrategicMergePatch => "application/strategic-merge-patch+json",
            V1Patch.PatchType.ApplyPatch => "application/apply-patch+yaml",
            _ => "application/json-patch+json",
        };

        private static void AddQueryParameter(StringBuilder sb, string key, string value)
        {
            if (string.IsNullOrEmpty(value))
                return;
            sb.Append(sb.Length != 0 ? '&' : '?').Append(Uri.EscapeDataString(key)).Append('=').Append(Uri.EscapeDataString(value));
        }
    }
}
