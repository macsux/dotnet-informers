using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase.Tests.Fakes
{
    /// <summary>
    ///     An in-memory <see cref="IKubernetesGenericClient" />: stores objects as JSON, applies merge patches (honouring a
    ///     <c>metadata.resourceVersion</c> precondition and the status subresource split), and records every write.
    /// </summary>
    internal sealed class FakeGenericClient : IKubernetesGenericClient
    {
        private readonly Dictionary<string, JsonObject> _objects = new Dictionary<string, JsonObject>();
        private long _version = 10;

        public bool HasStatus { get; set; } = true;

        public List<(string Verb, string Key, bool Status, string Body)> Writes { get; } = new List<(string, string, bool, string)>();

        public T Put<T>(T obj) where T : IKubernetesObject<V1ObjectMeta>
        {
            var json = JsonNode.Parse(InformerJson.Serialize(obj))!.AsObject();
            json["metadata"]!["resourceVersion"] = (++_version).ToString();
            json["metadata"]!["uid"] ??= Guid.NewGuid().ToString();
            _objects[obj.ObjectKey()] = json;
            return InformerJson.Deserialize<T>(json.ToJsonString());
        }

        public Task<T> GetAsync<T>(string name, string @namespace = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta> =>
            Task.FromResult(_objects.TryGetValue(KubernetesObjectExtensions.ObjectKey(@namespace, name), out var json) ? InformerJson.Deserialize<T>(json.ToJsonString()) : default);

        public Task<IReadOnlyList<T>> ListAsync<T>(string @namespace = null, string labelSelector = null, string fieldSelector = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta> =>
            Task.FromResult<IReadOnlyList<T>>(_objects.Values.Select(x => InformerJson.Deserialize<T>(x.ToJsonString())).ToList());

        public Task<T> CreateAsync<T>(T obj, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta>
        {
            if (obj.Metadata.Name == null && obj.Metadata.GenerateName != null)
                obj.Metadata.Name = obj.Metadata.GenerateName + Guid.NewGuid().ToString("N")[..5];
            if (_objects.ContainsKey(obj.ObjectKey()))
                throw Error(409, "AlreadyExists");
            Writes.Add(("create", obj.ObjectKey(), false, InformerJson.Serialize(obj)));
            return Task.FromResult(Put(obj));
        }

        public Task<T> ReplaceAsync<T>(T obj, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta>
        {
            Writes.Add(("replace", obj.ObjectKey(), status, InformerJson.Serialize(obj)));
            return Task.FromResult(Put(obj));
        }

        public Task<T> PatchAsync<T>(T obj, V1Patch patch, bool status = false, KubernetesWriteOptions options = null, CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            var key = obj.ObjectKey();
            var body = (string)patch.Content;
            Writes.Add(("patch", key, status, body));
            if (!_objects.TryGetValue(key, out var stored))
                throw Error(404, "NotFound");
            var patchJson = JsonNode.Parse(body)!.AsObject();
            var precondition = patchJson["metadata"]?["resourceVersion"]?.GetValue<string>();
            if (precondition != null && precondition != stored["metadata"]!["resourceVersion"]!.GetValue<string>())
                throw Error(409, "Conflict");
            if (HasStatus)
            {
                if (status)
                    patchJson = new JsonObject { ["status"] = patchJson["status"]?.DeepClone() };
                else
                    patchJson.Remove("status");
            }

            Merge(stored, patchJson);
            stored["metadata"]!["resourceVersion"] = (++_version).ToString();
            return Task.FromResult(InformerJson.Deserialize<T>(stored.ToJsonString()));
        }

        public Task<T> ApplyAsync<T>(T desired, string fieldManager, bool force = false, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta>
        {
            Writes.Add(("apply", desired.ObjectKey(), false, InformerJson.Serialize(desired)));
            return Task.FromResult(Put(desired));
        }

        public Task<bool> DeleteAsync<T>(T obj, V1DeleteOptions options = null, CancellationToken cancellationToken = default) where T : IKubernetesObject<V1ObjectMeta>
        {
            Writes.Add(("delete", obj.ObjectKey(), false, null));
            return Task.FromResult(_objects.Remove(obj.ObjectKey()));
        }

        public Task<KubernetesResourceInfo> GetResourceInfoAsync(IKubernetesObject<V1ObjectMeta> obj, CancellationToken cancellationToken = default) =>
            Task.FromResult(new KubernetesResourceInfo(new GroupVersionResource("", "v1", "things"), obj.Kind, true, HasStatus, false));

        public Task<KubernetesResourceInfo> GetResourceInfoAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) =>
            Task.FromResult(new KubernetesResourceInfo(resource, null, true, HasStatus, false));

        private static void Merge(JsonObject target, JsonObject patch)
        {
            foreach (var (name, value) in patch.ToList())
            {
                if (value == null)
                    target.Remove(name);
                else if (value is JsonObject nested && target[name] is JsonObject existing)
                    Merge(existing, nested);
                else
                    target[name] = value.DeepClone();
            }
        }

        private static Exception Error(int code, string reason) => new k8s.Autorest.HttpOperationException($"{code}")
        {
            Response = new k8s.Autorest.HttpResponseMessageWrapper(new System.Net.Http.HttpResponseMessage((System.Net.HttpStatusCode)code), $"{{\"kind\":\"Status\",\"code\":{code},\"reason\":\"{reason}\"}}"),
        };
    }
}
