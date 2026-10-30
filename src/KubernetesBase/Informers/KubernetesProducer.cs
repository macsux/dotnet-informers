using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Diagnostics;
using RetryPolicy = Steeltoe.Informers.InformersBase.FaultTolerance.RetryPolicy;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     The list/watch loop behind a Kubernetes informer: list (following <c>continue</c> tokens), apply the list as a whole, watch from its
    ///     <c>resourceVersion</c> (resuming from bookmarks), relist when the version is gone (410), retry transient errors with backoff.
    /// </summary>
    internal sealed class KubernetesProducer<T>
    {
        private static readonly Random Jitter = new Random();
        private readonly IKubernetesResourceClient _client;
        private readonly GroupVersionResource _resource;
        private readonly PayloadShape _shape;
        private readonly Func<JsonElement, T> _deserialize;
        private readonly RetryPolicy _retryPolicy;
        private readonly ILogger _logger;
        private readonly KeyValuePair<string, object> _nameTag;

        public KubernetesProducer(IKubernetesResourceClient client, GroupVersionResource resource, PayloadShape shape, Func<JsonElement, T> deserialize, RetryPolicy retryPolicy, ILogger logger)
        {
            _client = client;
            _resource = resource;
            _shape = shape;
            _deserialize = deserialize;
            _retryPolicy = retryPolicy ?? DefaultRetryPolicy;
            _logger = logger ?? NullLogger.Instance;
            _nameTag = InformerDiagnostics.Tag("informer", resource.ToString());
        }

        /// <summary>Objects per list page.</summary>
        public int PageSize { get; set; } = 500;

        /// <summary>The server ends each watch after a random time in this range (client-go uses 5-10 minutes); the producer then rewatches.</summary>
        public (int Min, int Max) WatchTimeoutSeconds { get; set; } = (300, 600);

        internal static RetryPolicy DefaultRetryPolicy { get; } = new RetryPolicy(
            (exception, attempt) => exception.IsTransient(),
            attempt => TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt - 1), 30)));

        public async Task RunAsync(IInformerSink<T> sink, ServerFilter filter, CancellationToken cancellationToken)
        {
            KubernetesSelection selection = null;
            string resourceVersion = null;
            var needList = true;
            var failures = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    // discovery (namespaced or not) is retried like list and watch
                    selection ??= await KubernetesSelection.ResolveAsync(_client, _resource, filter, cancellationToken).ConfigureAwait(false);
                    if (needList)
                    {
                        var (items, listVersion) = await ListAsync(selection, cancellationToken).ConfigureAwait(false);
                        sink.Replace(items);
                        resourceVersion = listVersion;
                        needList = false;
                        failures = 0;
                    }

                    // the callback records progress per event, so a dropped connection resumes from the last version seen
                    resourceVersion = await WatchAsync(sink, selection, resourceVersion, version =>
                    {
                        failures = 0;
                        if (version != null)
                            resourceVersion = version;
                    }, cancellationToken).ConfigureAwait(false);
                    InformerDiagnostics.WatchRestarts.Add(1, _nameTag);
                    _logger.LogDebug("Watch of {Resource} ended at {ResourceVersion}; rewatching", _resource, resourceVersion);
                }
                catch (Exception e) when (e.IsResourceVersionGone() && !cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Resource version {ResourceVersion} of {Resource} is gone; relisting", resourceVersion, _resource);
                    needList = true;
                    resourceVersion = null;
                }
                catch (Exception e) when (!cancellationToken.IsCancellationRequested && !(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
                {
                    failures++;
                    if (!_retryPolicy.ShouldRetry(e, failures))
                        throw;
                    InformerDiagnostics.ProducerErrors.Add(1, _nameTag);
                    var delay = _retryPolicy.RetryDelay(failures);
                    _logger.LogWarning(e, "List/watch of {Resource} failed (attempt {Attempt}); retrying in {Delay}", _resource, failures, delay);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    InformerDiagnostics.WatchRestarts.Add(1, _nameTag);
                }
            }
        }

        /// <summary>Lists every page; restarts the list if a continue token expires. The pages are applied together, as one list.</summary>
        internal async Task<(List<T> Items, string ResourceVersion)> ListAsync(KubernetesSelection selection, CancellationToken cancellationToken)
        {
            while (true)
            {
                var items = new List<T>();
                string continueToken = null;
                string resourceVersion;
                try
                {
                    do
                    {
                        var request = new KubernetesListRequest
                        {
                            LabelSelector = selection.LabelSelector,
                            FieldSelector = selection.FieldSelector,
                            Limit = PageSize > 0 ? PageSize : null,
                            Continue = continueToken,
                            Accept = _shape == PayloadShape.MetadataOnly ? KubernetesListRequest.MetadataListAccept : null,
                        };
                        using var response = await _client.SendListAsync(_resource, selection.Namespace, request, cancellationToken).ConfigureAwait(false);
                        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
                        var root = document.RootElement;
                        if (root.TryGetProperty("items", out var array) && array.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var element in array.EnumerateArray())
                                items.Add(_deserialize(element));
                        }

                        var metadata = root.TryGetProperty("metadata", out var m) ? m : default;
                        resourceVersion = metadata.ValueKind == JsonValueKind.Object && metadata.TryGetProperty("resourceVersion", out var rv) ? rv.GetString() : null;
                        continueToken = metadata.ValueKind == JsonValueKind.Object && metadata.TryGetProperty("continue", out var c) ? c.GetString() : null;
                    }
                    while (!string.IsNullOrEmpty(continueToken));
                }
                catch (Exception e) when (continueToken != null && e.IsResourceVersionGone())
                {
                    _logger.LogInformation("Continue token of {Resource} list expired; restarting the list", _resource);
                    continue;
                }

                return (items, resourceVersion);
            }
        }

        /// <summary>Applies watch events until the server ends the watch; returns the last resource version seen.</summary>
        private async Task<string> WatchAsync(IInformerSink<T> sink, KubernetesSelection selection, string resourceVersion, Action<string> onEvent, CancellationToken cancellationToken)
        {
            int timeout;
            lock (Jitter)
                timeout = Jitter.Next(WatchTimeoutSeconds.Min, WatchTimeoutSeconds.Max + 1);
            var request = new KubernetesListRequest
            {
                LabelSelector = selection.LabelSelector,
                FieldSelector = selection.FieldSelector,
                ResourceVersion = resourceVersion,
                Watch = true,
                AllowWatchBookmarks = true,
                TimeoutSeconds = timeout,
                Accept = _shape == PayloadShape.MetadataOnly ? KubernetesListRequest.MetadataAccept : null,
            };
            using var response = await _client.SendListAsync(_resource, selection.Namespace, request, cancellationToken).ConfigureAwait(false);
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var reader = new StreamReader(stream);
            string line;
            while ((line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false)) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                JsonDocument document;
                try
                {
                    document = JsonDocument.Parse(line);
                }
                catch (JsonException e)
                {
                    // a stream cut off mid-event: a broken connection, retried like one
                    throw new IOException("The watch stream ended with an incomplete event.", e);
                }

                using var _ = document;
                var root = document.RootElement;
                var type = root.GetProperty("type").GetString();
                var obj = root.GetProperty("object");
                if (type == "ERROR")
                    throw new KubernetesException(KubernetesJson.Deserialize<V1Status>(obj.GetRawText()));
                var version = obj.TryGetProperty("metadata", out var metadata) && metadata.TryGetProperty("resourceVersion", out var rv) ? rv.GetString() : null;
                switch (type)
                {
                    case "ADDED":
                    case "MODIFIED":
                        sink.Upsert(_deserialize(obj));
                        break;
                    case "DELETED":
                        sink.Delete(_deserialize(obj));
                        break;
                    case "BOOKMARK":
                        break;
                    default:
                        throw new KubernetesException($"Unknown watch event type '{type}'");
                }

                if (version != null)
                    resourceVersion = version;
                onEvent(version);
            }

            return resourceVersion;
        }
    }

    /// <summary>A server filter translated into the namespace and selectors of a list/watch request.</summary>
    internal sealed class KubernetesSelection
    {
        public string Namespace { get; private set; }
        public string LabelSelector { get; private set; }
        public string FieldSelector { get; private set; }

        public static async Task<KubernetesSelection> ResolveAsync(IKubernetesResourceClient client, GroupVersionResource resource, ServerFilter filter, CancellationToken cancellationToken)
        {
            var selection = new KubernetesSelection();
            var fields = filter.ValuesOf(KubernetesFilterTranslator.Field).ToList();
            var labels = filter.ValuesOf(KubernetesFilterTranslator.Label).ToList();
            var namespaces = filter.ValuesOf(KubernetesFilterTranslator.Namespace).ToList();
            if (namespaces.Count > 1)
                throw new InvalidOperationException("A server filter can't select more than one namespace"); // the translator never produces this
            if (namespaces.Count == 1)
            {
                // a namespace filter on a cluster-scoped resource matches nothing, the same as a field selector would say
                if (await client.IsNamespacedAsync(resource, cancellationToken).ConfigureAwait(false))
                    selection.Namespace = namespaces[0];
                else
                    fields.Add($"metadata.namespace={namespaces[0]}");
            }

            selection.LabelSelector = labels.Count > 0 ? string.Join(",", labels) : null;
            selection.FieldSelector = fields.Count > 0 ? string.Join(",", fields) : null;
            return selection;
        }
    }
}
