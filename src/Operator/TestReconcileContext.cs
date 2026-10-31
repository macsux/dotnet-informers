using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    public static class ReconcileContext
    {
        /// <summary>
        ///     A context for unit-testing a reconcile without informers or an API server: built from plain objects, it records writes
        ///     (<see cref="TestReconcileContext{T}.Writes" />) instead of sending them and applies them to an in-memory store.
        /// </summary>
        public static TestReconcileContext<T> ForTest<T>(T obj, ReconcileTrigger trigger = ReconcileTrigger.Change) where T : class, IKubernetesObject<V1ObjectMeta> =>
            new TestReconcileContext<T>(obj, trigger, new TestKubernetesClient(), new List<RelatedSet>());
    }

    /// <summary>A write a <see cref="TestReconcileContext{T}" /> recorded.</summary>
    /// <param name="Verb"><c>create</c>, <c>patch</c>, <c>replace</c>, <c>apply</c> or <c>delete</c></param>
    /// <param name="Kind">The object's kind</param>
    /// <param name="Name">The object's name (generated for <c>generateName</c> creates)</param>
    /// <param name="Status">Whether it went to the <c>/status</c> subresource</param>
    /// <param name="Body">The object (create, replace, apply) or patch sent, as JSON</param>
    public sealed record RecordedWrite(string Verb, string Kind, string Name, bool Status, string Body);

    /// <summary>A <see cref="ReconcileContext{T}" /> over plain objects that records writes (see <see cref="ReconcileContext.ForTest{T}" />).</summary>
    public sealed class TestReconcileContext<T> : ReconcileContext<T> where T : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly List<RelatedSet> _related;
        private readonly TestKubernetesClient _client;

        internal TestReconcileContext(T obj, ReconcileTrigger trigger, TestKubernetesClient client, List<RelatedSet> related)
            : base(obj.ObjectKey(), trigger, client.Store(obj), related, client, new Expectations(TimeSpan.FromMinutes(5)), NullEventRecorder.Instance, NullLogger.Instance)
        {
            _client = client;
            _related = related;
        }

        /// <summary>Every write the reconcile made, in order.</summary>
        public IReadOnlyList<RecordedWrite> Writes => _client.Writes;

        /// <summary>Owned children (as declared by <c>Owns&lt;TChild&gt;()</c>).</summary>
        public TestReconcileContext<T> WithOwned<TChild>(params TChild[] children) where TChild : class, IKubernetesObject<V1ObjectMeta> =>
            With(RelationshipKind.Owns, null, children);

        /// <summary>Related objects (as declared by <c>References&lt;TOther&gt;</c> or <c>Watches&lt;TOther&gt;</c>), optionally named.</summary>
        public TestReconcileContext<T> WithRelated<TOther>(params TOther[] others) where TOther : class, IKubernetesObject<V1ObjectMeta> =>
            With(RelationshipKind.References, null, others);

        /// <summary>Related objects of the reference named <paramref name="name" />.</summary>
        public TestReconcileContext<T> WithRelated<TOther>(string name, params TOther[] others) where TOther : class, IKubernetesObject<V1ObjectMeta> =>
            With(RelationshipKind.References, name, others);

        /// <summary>Makes writes behave as for a resource without a status subresource (status changes go to the main resource).</summary>
        public TestReconcileContext<T> WithoutStatusSubresource()
        {
            _client.HasStatus = false;
            return this;
        }

        private TestReconcileContext<T> With<TOther>(RelationshipKind kind, string name, TOther[] items) where TOther : class, IKubernetesObject<V1ObjectMeta>
        {
            var set = _related.OfType<RelatedSet<TOther>>().FirstOrDefault(x => x.Kind == kind && x.Name == name);
            if (set == null)
                _related.Add(set = new RelatedSet<TOther>(kind, name, Array.Empty<TOther>()));
            set.Items.AddRange(items.Select(x => new Tracked<TOther>(_client.Store(x))));
            return this;
        }
    }

    /// <summary>An in-memory API server for <see cref="TestReconcileContext{T}" />: applies writes to stored JSON and records them.</summary>
    internal sealed class TestKubernetesClient : IKubernetesGenericClient
    {
        private readonly object _lock = new object();
        private readonly Dictionary<(string Kind, string Key), JsonObject> _objects = new Dictionary<(string, string), JsonObject>();
        private readonly List<RecordedWrite> _writes = new List<RecordedWrite>();
        private long _version = 1000;
        private int _names;

        public bool HasStatus { get; set; } = true;

        public IReadOnlyList<RecordedWrite> Writes
        {
            get
            {
                lock (_lock)
                    return _writes.ToList();
            }
        }

        /// <summary>Stores an object (filling in a uid and resourceVersion when missing) and returns it as stored.</summary>
        public TObject Store<TObject>(TObject obj) where TObject : IKubernetesObject<V1ObjectMeta>
        {
            var json = Patching.ToJson(obj);
            var (apiVersion, kind) = obj.TypeMeta();
            json["apiVersion"] ??= apiVersion;
            json["kind"] ??= kind;
            var metadata = json["metadata"] as JsonObject ?? (JsonObject)(json["metadata"] = new JsonObject());
            lock (_lock)
            {
                metadata["uid"] ??= Guid.NewGuid().ToString();
                metadata["resourceVersion"] ??= (++_version).ToString();
                _objects[(kind, obj.ObjectKey())] = json;
            }

            return InformerJson.Deserialize<TObject>(json.ToJsonString());
        }

        public Task<TObject> GetAsync<TObject>(string name, string @namespace = null, CancellationToken cancellationToken = default) where TObject : IKubernetesObject<V1ObjectMeta>
        {
            var kind = Kind<TObject>();
            lock (_lock)
                return Task.FromResult(_objects.TryGetValue((kind, KubernetesObjectExtensions.ObjectKey(@namespace, name)), out var json) ? InformerJson.Deserialize<TObject>(json.ToJsonString()) : default);
        }

        public Task<IReadOnlyList<TObject>> ListAsync<TObject>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
            where TObject : IKubernetesObject<V1ObjectMeta>
        {
            var kind = Kind<TObject>();
            lock (_lock)
            {
                return Task.FromResult<IReadOnlyList<TObject>>(_objects.Where(x => x.Key.Kind == kind && (@namespace == null || x.Key.Key.StartsWith(@namespace + "/", StringComparison.Ordinal)))
                    .Select(x => InformerJson.Deserialize<TObject>(x.Value.ToJsonString())).ToList());
            }
        }

        public Task<TObject> CreateAsync<TObject>(TObject obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default) where TObject : IKubernetesObject<V1ObjectMeta>
        {
            var json = Patching.ToJson(obj);
            var metadata = (JsonObject)json["metadata"];
            if (metadata["name"] == null && metadata["generateName"] != null)
                metadata["name"] = metadata["generateName"]!.GetValue<string>() + Interlocked.Increment(ref _names).ToString("x5");
            var created = InformerJson.Deserialize<TObject>(json.ToJsonString());
            Record("create", created, false, json.ToJsonString());
            return Task.FromResult(Store(created));
        }

        public Task<TObject> ReplaceAsync<TObject>(TObject obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where TObject : IKubernetesObject<V1ObjectMeta>
        {
            Record("replace", obj, status, InformerJson.Serialize(obj));
            obj.Metadata.ResourceVersion = null;
            return Task.FromResult(Store(obj));
        }

        public Task<TObject> PatchAsync<TObject>(TObject obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where TObject : IKubernetesObject<V1ObjectMeta>
        {
            var body = patch.Content as string ?? InformerJson.Serialize(patch.Content);
            Record("patch", obj, status, body);
            var (_, kind) = obj.TypeMeta();
            lock (_lock)
            {
                if (!_objects.TryGetValue((kind, obj.ObjectKey()), out var stored))
                    stored = Patching.ToJson(obj);
                var patchJson = (JsonObject)JsonNode.Parse(body);
                if (HasStatus)
                {
                    if (status)
                        patchJson = new JsonObject { ["status"] = patchJson["status"]?.DeepClone() };
                    else
                        patchJson.Remove("status");
                }

                ((JsonObject)patchJson["metadata"])?.Remove("resourceVersion");
                JsonMergePatch.Apply(stored, patchJson);
                stored["metadata"]!["resourceVersion"] = (++_version).ToString();
                _objects[(kind, obj.ObjectKey())] = stored;
                return Task.FromResult(InformerJson.Deserialize<TObject>(stored.ToJsonString()));
            }
        }

        public Task<TObject> ApplyAsync<TObject>(TObject desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default) where TObject : IKubernetesObject<V1ObjectMeta>
        {
            Record("apply", desired, false, InformerJson.Serialize(desired));
            return Task.FromResult(Store(desired));
        }

        public Task<bool> DeleteAsync<TObject>(TObject obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default) where TObject : IKubernetesObject<V1ObjectMeta>
        {
            Record("delete", obj, false, null);
            var (_, kind) = obj.TypeMeta();
            lock (_lock)
                return Task.FromResult(_objects.Remove((kind, obj.ObjectKey())));
        }

        public Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default) =>
            Task.FromResult(new KubernetesResourceInfo(new GroupVersionResource(string.Empty, "v1", "objects"), obj.TypeMeta().Kind, true, HasStatus, false));

        public Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) =>
            Task.FromResult(new KubernetesResourceInfo(resource, null, true, HasStatus, false));

        private void Record(string verb, IKubernetesObject<V1ObjectMeta> obj, bool status, string body)
        {
            lock (_lock)
                _writes.Add(new RecordedWrite(verb, obj.TypeMeta().Kind, obj.Metadata?.Name, status, body));
        }

        private static string Kind<TObject>() where TObject : IKubernetesObject<V1ObjectMeta> =>
            ((k8s.Models.KubernetesEntityAttribute)Attribute.GetCustomAttribute(typeof(TObject), typeof(k8s.Models.KubernetesEntityAttribute)))?.Kind ?? typeof(TObject).Name;
    }
}
