using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Buffers;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;

namespace Steeltoe.Informers.KubernetesBase.Tests.Fakes
{
    /// <summary>
    ///     An in-memory API server behind <see cref="IKubernetesGenericClient" />: list with label/field selectors and pagination, watch from a
    ///     resource version with bookmarks and selector transitions (objects leaving a selector arrive as DELETED, like the real watch cache),
    ///     compaction (410 Gone), dropped connections, and a log of every request.
    /// </summary>
    internal sealed class FakeApiServer : IKubernetesResourceClient
    {
        private readonly object _lock = new object();
        private readonly Dictionary<GroupVersionResource, Store> _stores = new Dictionary<GroupVersionResource, Store>();
        private readonly HashSet<GroupVersionResource> _clusterScoped = new HashSet<GroupVersionResource>();
        private readonly List<Watch> _watches = new List<Watch>();
        private long _revision = 100;
        private long _compactedRevision;

        public ConcurrentQueue<(GroupVersionResource Resource, string Namespace, KubernetesListRequest Request)> Requests { get; } = new ConcurrentQueue<(GroupVersionResource, string, KubernetesListRequest)>();

        /// <summary>When set, the next list/watch requests throw it (e.g. an HTTP error), once per entry.</summary>
        public ConcurrentQueue<Exception> Failures { get; } = new ConcurrentQueue<Exception>();

        /// <summary>Compaction is reported to open watches as a watch ERROR event (true) or by closing them (false).</summary>
        public bool CompactionAsWatchError { get; set; } = true;

        public int OpenWatches
        {
            get
            {
                lock (_lock)
                    return _watches.Count;
            }
        }

        public int ListRequests => Requests.Count(x => !x.Request.Watch);
        public int WatchRequests => Requests.Count(x => x.Request.Watch);

        public void ClusterScoped(GroupVersionResource resource) => _clusterScoped.Add(resource);

        private sealed class Store
        {
            public readonly SortedDictionary<string, JsonObject> Objects = new SortedDictionary<string, JsonObject>(StringComparer.Ordinal);
        }

        private sealed class Watch
        {
            public GroupVersionResource Resource;
            public string Namespace;
            public Selector Selector;
            public Pipe Pipe;
            public bool Bookmarks;
            public string Accept;
        }

        public string Put(GroupVersionResource resource, object obj)
        {
            var node = JsonNode.Parse(KubernetesJson.Serialize(obj))!.AsObject();
            lock (_lock)
            {
                var store = StoreOf(resource);
                var key = KeyOf(node);
                store.Objects.TryGetValue(key, out var old);
                var rv = (++_revision).ToString();
                var metadata = node["metadata"]!.AsObject();
                metadata["resourceVersion"] = rv;
                metadata["uid"] ??= old?["metadata"]?["uid"]?.GetValue<string>() ?? Guid.NewGuid().ToString();
                store.Objects[key] = node;
                Broadcast(resource, old, node, false);
                return rv;
            }
        }

        public void Remove(GroupVersionResource resource, string @namespace, string name)
        {
            lock (_lock)
            {
                var store = StoreOf(resource);
                var key = KubernetesObjectExtensions.ObjectKey(@namespace, name);
                if (!store.Objects.Remove(key, out var old))
                    return;
                var deleted = old.DeepClone().AsObject();
                deleted["metadata"]!["resourceVersion"] = (++_revision).ToString();
                Broadcast(resource, old, deleted, true);
            }
        }

        public JsonObject Get(GroupVersionResource resource, string @namespace, string name)
        {
            lock (_lock)
                return StoreOf(resource).Objects.TryGetValue(KubernetesObjectExtensions.ObjectKey(@namespace, name), out var obj) ? obj : null;
        }

        /// <summary>Forgets history: watches from older versions get 410 Gone, open watches end (with an ERROR event or a closed stream).</summary>
        public void Compact()
        {
            lock (_lock)
            {
                _compactedRevision = _revision;
                foreach (var watch in _watches.ToList())
                {
                    if (CompactionAsWatchError)
                        WriteLine(watch, "{\"type\":\"ERROR\",\"object\":{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\",\"message\":\"too old resource version\",\"reason\":\"Expired\",\"code\":410}}");
                    watch.Pipe.Writer.Complete();
                }

                _watches.Clear();
            }
        }

        /// <summary>Drops every open watch connection (the reader sees an IOException).</summary>
        public void DropConnections()
        {
            lock (_lock)
            {
                foreach (var watch in _watches)
                    watch.Pipe.Writer.Complete(new IOException("connection reset"));
                _watches.Clear();
            }
        }

        /// <summary>Ends every open watch normally, like a server-side timeout, after sending a bookmark.</summary>
        public void EndWatches()
        {
            lock (_lock)
            {
                foreach (var watch in _watches)
                {
                    if (watch.Bookmarks)
                        WriteLine(watch, $"{{\"type\":\"BOOKMARK\",\"object\":{{\"kind\":\"Pod\",\"apiVersion\":\"v1\",\"metadata\":{{\"resourceVersion\":\"{_revision}\"}}}}}}");
                    watch.Pipe.Writer.Complete();
                }

                _watches.Clear();
            }
        }

        public Task<bool> IsNamespacedAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) => Task.FromResult(!_clusterScoped.Contains(resource));

        public Task<HttpResponseMessage> SendListAsync(GroupVersionResource resource, string namespaceParameter, KubernetesListRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Enqueue((resource, namespaceParameter, request));
            if (Failures.TryDequeue(out var failure))
                throw failure;
            var selector = new Selector(request.LabelSelector, request.FieldSelector);
            lock (_lock)
            {
                return Task.FromResult(request.Watch ? StartWatch(resource, namespaceParameter, request, selector) : List(resource, namespaceParameter, request, selector));
            }
        }

        private HttpResponseMessage List(GroupVersionResource resource, string @namespace, KubernetesListRequest request, Selector selector)
        {
            var offset = 0;
            if (request.Continue != null)
            {
                var parts = request.Continue.Split(':');
                if (long.Parse(parts[1]) < _compactedRevision)
                    throw Gone();
                offset = int.Parse(parts[0]);
            }

            var all = StoreOf(resource).Objects.Values.Where(x => Matches(x, @namespace, selector)).ToList();
            var page = request.Limit is { } limit ? all.Skip(offset).Take(limit).ToList() : all.Skip(offset).ToList();
            var next = offset + page.Count;
            var list = new JsonObject
            {
                ["kind"] = "List",
                ["apiVersion"] = "v1",
                ["metadata"] = new JsonObject
                {
                    ["resourceVersion"] = _revision.ToString(),
                    ["continue"] = next < all.Count ? $"{next}:{_revision}" : null,
                },
                ["items"] = new JsonArray(page.Select(x => Shape(x, request.Accept)).ToArray<JsonNode>()),
            };
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(list.ToJsonString(), Encoding.UTF8, "application/json") };
        }

        private HttpResponseMessage StartWatch(GroupVersionResource resource, string @namespace, KubernetesListRequest request, Selector selector)
        {
            if (request.ResourceVersion != null && long.Parse(request.ResourceVersion) < _compactedRevision)
                throw Gone();
            var watch = new Watch { Resource = resource, Namespace = @namespace, Selector = selector, Pipe = new Pipe(), Bookmarks = request.AllowWatchBookmarks == true };
            watch.Accept = request.Accept;
            _watches.Add(watch);
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(watch.Pipe.Reader.AsStream()) };
        }

        private void Broadcast(GroupVersionResource resource, JsonObject old, JsonObject current, bool deleted)
        {
            foreach (var watch in _watches.Where(x => x.Resource == resource))
            {
                var wasIn = old != null && Matches(old, watch.Namespace, watch.Selector);
                var isIn = !deleted && Matches(current, watch.Namespace, watch.Selector);
                string type;
                if (deleted)
                    type = wasIn ? "DELETED" : null;
                else if (wasIn && isIn)
                    type = "MODIFIED";
                else if (isIn)
                    type = "ADDED";
                else if (wasIn)
                    type = "DELETED"; // left the selector: the watch cache sends the new object as DELETED
                else
                    type = null;
                if (type != null)
                    WriteLine(watch, new JsonObject { ["type"] = type, ["object"] = Shape(current, watch.Accept) }.ToJsonString());
            }
        }

        private static JsonNode Shape(JsonObject obj, string accept)
        {
            if (accept == null || !accept.Contains("PartialObjectMetadata"))
                return obj.DeepClone();
            return new JsonObject
            {
                ["kind"] = "PartialObjectMetadata",
                ["apiVersion"] = "meta.k8s.io/v1",
                ["metadata"] = obj["metadata"]!.DeepClone(),
            };
        }

        private static void WriteLine(Watch watch, string line)
        {
            var bytes = Encoding.UTF8.GetBytes(line + "\n");
            watch.Pipe.Writer.Write(bytes);
            _ = watch.Pipe.Writer.FlushAsync();
        }

        private bool Matches(JsonObject obj, string @namespace, Selector selector)
        {
            var objNamespace = obj["metadata"]?["namespace"]?.GetValue<string>();
            if (!string.IsNullOrEmpty(@namespace) && objNamespace != @namespace)
                return false;
            return selector.Matches(obj);
        }

        private static string KeyOf(JsonObject obj) =>
            KubernetesObjectExtensions.ObjectKey(obj["metadata"]?["namespace"]?.GetValue<string>(), obj["metadata"]!["name"]!.GetValue<string>());

        private Store StoreOf(GroupVersionResource resource)
        {
            if (!_stores.TryGetValue(resource, out var store))
                _stores[resource] = store = new Store();
            return store;
        }

        private static HttpOperationException Gone() => new HttpOperationException("Gone")
        {
            Response = new HttpResponseMessageWrapper(new HttpResponseMessage(HttpStatusCode.Gone), "{\"kind\":\"Status\",\"code\":410}"),
        };

        /// <summary>Label and field selector evaluation with API server semantics.</summary>
        internal sealed class Selector
        {
            private readonly List<Func<JsonObject, bool>> _requirements = new List<Func<JsonObject, bool>>();

            public Selector(string labelSelector, string fieldSelector)
            {
                if (!string.IsNullOrEmpty(labelSelector))
                {
                    foreach (var requirement in KubernetesInformer<k8s.Models.V1Pod>.SplitSelector(labelSelector))
                        _requirements.Add(ParseLabel(requirement));
                }

                if (!string.IsNullOrEmpty(fieldSelector))
                {
                    foreach (var requirement in fieldSelector.Split(','))
                        _requirements.Add(ParseField(requirement));
                }
            }

            public bool Matches(JsonObject obj) => _requirements.All(x => x(obj));

            private static string Label(JsonObject obj, string key) => obj["metadata"]?["labels"]?[key]?.GetValue<string>();

            private static Func<JsonObject, bool> ParseLabel(string requirement)
            {
                requirement = requirement.Trim();
                if (requirement.Contains(" notin ") || requirement.Contains(" in "))
                {
                    var notIn = requirement.Contains(" notin ");
                    var parts = requirement.Split(notIn ? " notin " : " in ");
                    var key = parts[0].Trim();
                    var values = parts[1].Trim().Trim('(', ')').Split(',').Select(x => x.Trim()).ToHashSet();
                    return notIn ? o => !values.Contains(Label(o, key)) : o => Label(o, key) is { } v && values.Contains(v);
                }

                if (requirement.Contains("!="))
                {
                    var parts = requirement.Split("!=");
                    return o => Label(o, parts[0]) != parts[1];
                }

                if (requirement.Contains("="))
                {
                    var parts = requirement.Split('=', 2);
                    var value = parts[1].TrimStart('=');
                    return o => Label(o, parts[0]) == value;
                }

                if (requirement.StartsWith("!"))
                    return o => Label(o, requirement.Substring(1)) == null;
                return o => Label(o, requirement) != null;
            }

            private static Func<JsonObject, bool> ParseField(string requirement)
            {
                var notEqual = requirement.Contains("!=");
                var parts = notEqual ? requirement.Split("!=") : requirement.Split('=', 2);
                var path = parts[0].Split('.');
                var expected = parts[1].TrimStart('=');
                return o =>
                {
                    JsonNode node = o;
                    foreach (var segment in path)
                        node = node?[segment];
                    var actual = node?.GetValue<object>()?.ToString() ?? string.Empty;
                    return notEqual ? actual != expected : actual == expected;
                };
            }
        }
    }
}
