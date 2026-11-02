using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator.Web
{
    internal enum WebhookKind
    {
        Validating,
        Mutating,
        Converting,
    }

    /// <summary>A registered webhook: what it serves and how to handle one request (typed by the generic interface it implements).</summary>
    internal sealed class WebhookDescriptor
    {
        public WebhookKind Kind { get; private init; }
        public Type ImplementationType { get; private init; }
        public GroupVersionResource Resource { get; private init; }
        public string EntityKind { get; private init; }
        public IReadOnlyList<string> Operations { get; private init; } = Array.Empty<string>();

        /// <summary>Admission: handles one request for the resolved implementation.</summary>
        public Func<object, AdmissionRequest, CancellationToken, Task<AdmissionResponse>> Admit { get; private init; }

        /// <summary>Conversion (spoke side): the spoke's apiVersion, and its conversions to and from the hub.</summary>
        public string SpokeApiVersion { get; private init; }
        public string HubApiVersion { get; private init; }
        public Func<object, JsonElement, object> ToHub { get; private init; }
        public Func<object, object, JsonObject> FromHub { get; private init; }
        public Func<JsonElement, object> ReadHub { get; private init; }
        public Func<object, JsonObject> WriteHub { get; private init; }

        /// <summary>The URL path Kubernetes calls.</summary>
        public string Path => Kind switch
        {
            WebhookKind.Validating => $"/validate/{GroupSegment}/{Resource.Version}/{Resource.Resource}",
            WebhookKind.Mutating => $"/mutate/{GroupSegment}/{Resource.Version}/{Resource.Resource}",
            _ => $"/convert/{GroupSegment}/{Resource.Resource}",
        };

        private string GroupSegment => string.IsNullOrEmpty(Resource.Group) ? "core" : Resource.Group;

        public static WebhookDescriptor Validating<T>(Type implementationType, IReadOnlyList<string> operations) where T : class, IKubernetesObject<V1ObjectMeta> =>
            new WebhookDescriptor
            {
                Kind = WebhookKind.Validating,
                ImplementationType = implementationType,
                Resource = GroupVersionResource.For<T>(),
                EntityKind = KindOf<T>(),
                Operations = operations,
                Admit = async (instance, request, ct) =>
                {
                    var validator = (IValidator<T>)instance;
                    var context = Context(request);
                    var result = request.Operation switch
                    {
                        "CREATE" => await validator.CreateAsync(Read<T>(request.Object), context, ct).ConfigureAwait(false),
                        "UPDATE" => await validator.UpdateAsync(Read<T>(request.OldObject), Read<T>(request.Object), context, ct).ConfigureAwait(false),
                        "DELETE" => await validator.DeleteAsync(Read<T>(request.OldObject), context, ct).ConfigureAwait(false),
                        _ => AdmissionResult.Allow(),
                    };
                    return new AdmissionResponse
                    {
                        Uid = request.Uid,
                        Allowed = result.Allowed,
                        Status = result.Allowed ? null : new ReviewStatus { Status = "Failure", Code = result.StatusCode, Message = result.Message },
                        Warnings = result.Warnings.Count > 0 ? result.Warnings.ToList() : null,
                    };
                },
            };

        public static WebhookDescriptor Mutating<T>(Type implementationType, IReadOnlyList<string> operations) where T : class, IKubernetesObject<V1ObjectMeta> =>
            new WebhookDescriptor
            {
                Kind = WebhookKind.Mutating,
                ImplementationType = implementationType,
                Resource = GroupVersionResource.For<T>(),
                EntityKind = KindOf<T>(),
                Operations = operations,
                Admit = async (instance, request, ct) =>
                {
                    var mutator = (IMutator<T>)instance;
                    var context = Context(request);
                    var obj = Read<T>(request.Object);
                    var original = Patching.ToJson(obj);
                    var mutated = request.Operation switch
                    {
                        "CREATE" => await mutator.CreateAsync(obj, context, ct).ConfigureAwait(false),
                        "UPDATE" => await mutator.UpdateAsync(Read<T>(request.OldObject), obj, context, ct).ConfigureAwait(false),
                        _ => obj,
                    };
                    // what the handler changed (both sides through the same serializer), applied to the object as the API server sent it: the
                    // JSON patch must address that object, which may lack members the typed round trip always writes (an int left out)
                    var changes = JsonMergePatch.Create(original, Patching.ToJson(mutated ?? obj));
                    var patch = new JsonArray();
                    if (changes != null)
                    {
                        var raw = JsonNode.Parse(request.Object.Value.GetRawText())!.AsObject();
                        var result = (JsonObject)raw.DeepClone();
                        JsonMergePatch.Apply(result, changes);
                        patch = JsonPatch.Create(raw, result);
                    }

                    return new AdmissionResponse
                    {
                        Uid = request.Uid,
                        Allowed = true,
                        PatchType = patch.Count > 0 ? "JSONPatch" : null,
                        Patch = patch.Count > 0 ? Convert.ToBase64String(Encoding.UTF8.GetBytes(patch.ToJsonString())) : null,
                    };
                },
            };

        public static WebhookDescriptor Converting<TSpoke, THub>(Type implementationType)
            where TSpoke : class, IKubernetesObject<V1ObjectMeta>
            where THub : class, IKubernetesObject<V1ObjectMeta> =>
            new WebhookDescriptor
            {
                Kind = WebhookKind.Converting,
                ImplementationType = implementationType,
                Resource = GroupVersionResource.For<THub>(),
                EntityKind = KindOf<THub>(),
                SpokeApiVersion = GroupVersionResource.For<TSpoke>().ApiVersion,
                HubApiVersion = GroupVersionResource.For<THub>().ApiVersion,
                ToHub = (instance, json) => ((IConverter<TSpoke, THub>)instance).ToHub(Read<TSpoke>(json)),
                FromHub = (instance, hub) => WithTypeMeta(Patching.ToJson(((IConverter<TSpoke, THub>)instance).FromHub((THub)hub)), GroupVersionResource.For<TSpoke>().ApiVersion, KindOf<TSpoke>()),
                ReadHub = json => Read<THub>(json),
                WriteHub = hub => WithTypeMeta(Patching.ToJson(hub), GroupVersionResource.For<THub>().ApiVersion, KindOf<THub>()),
            };

        private static T Read<T>(JsonElement? json) => json is { ValueKind: JsonValueKind.Object } element ? InformerJson.Deserialize<T>(element) : default;

        private static AdmissionContext Context(AdmissionRequest request) => new AdmissionContext
        {
            Operation = request.Operation,
            Namespace = request.Namespace,
            Name = request.Name,
            SubResource = request.SubResource,
            UserInfo = request.UserInfo,
            DryRun = request.DryRun == true,
        };

        private static JsonObject WithTypeMeta(JsonObject json, string apiVersion, string kind)
        {
            json["apiVersion"] = apiVersion;
            json["kind"] = kind;
            return json;
        }

        private static string KindOf<T>() =>
            ((KubernetesEntityAttribute)Attribute.GetCustomAttribute(typeof(T), typeof(KubernetesEntityAttribute)))?.Kind ?? typeof(T).Name;
    }

    /// <summary>Computes a JSON Patch (RFC 6902): objects member by member, everything else replaced whole.</summary>
    internal static class JsonPatch
    {
        public static JsonArray Create(JsonNode before, JsonNode after)
        {
            var operations = new JsonArray();
            Diff(before, after, string.Empty, operations);
            return operations;
        }

        private static void Diff(JsonNode before, JsonNode after, string path, JsonArray operations)
        {
            if (before is JsonObject beforeObject && after is JsonObject afterObject)
            {
                foreach (var (name, value) in beforeObject)
                {
                    if (!afterObject.ContainsKey(name))
                        operations.Add((JsonNode)new JsonObject { ["op"] = "remove", ["path"] = $"{path}/{Escape(name)}" });
                }

                foreach (var (name, value) in afterObject)
                {
                    if (!beforeObject.TryGetPropertyValue(name, out var old))
                        operations.Add((JsonNode)new JsonObject { ["op"] = "add", ["path"] = $"{path}/{Escape(name)}", ["value"] = value?.DeepClone() });
                    else
                        Diff(old, value, $"{path}/{Escape(name)}", operations);
                }

                return;
            }

            if (!JsonNode.DeepEquals(before, after))
                operations.Add((JsonNode)new JsonObject { ["op"] = "replace", ["path"] = path, ["value"] = after?.DeepClone() });
        }

        private static string Escape(string name) => name.Replace("~", "~0").Replace("/", "~1");
    }
}
