using System;
using System.Text.Json;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     A Kubernetes object without a C# type: typed standard metadata plus the raw JSON of the whole object. Read fields from
    ///     <see cref="Raw" /> (e.g. <c>Raw.GetProperty("spec")</c>); it's immutable, so it can be shared safely.
    /// </summary>
    public sealed class UntypedKubernetesObject : IKubernetesObject<V1ObjectMeta>
    {
        public UntypedKubernetesObject(string apiVersion, string kind, V1ObjectMeta metadata, JsonElement raw)
        {
            ApiVersion = apiVersion;
            Kind = kind;
            Metadata = metadata;
            Raw = raw;
        }

        public string ApiVersion { get; set; }
        public string Kind { get; set; }
        public V1ObjectMeta Metadata { get; set; }

        /// <summary>The whole object as returned by the API server.</summary>
        public JsonElement Raw { get; }

        /// <summary>A top-level section of the object (e.g. <c>spec</c>), or <see langword="null" /> if absent.</summary>
        public JsonElement? Get(string property) => Raw.ValueKind == JsonValueKind.Object && Raw.TryGetProperty(property, out var value) ? value : null;

        public static UntypedKubernetesObject FromJson(JsonElement element)
        {
            var apiVersion = element.TryGetProperty("apiVersion", out var a) ? a.GetString() : null;
            var kind = element.TryGetProperty("kind", out var k) ? k.GetString() : null;
            var metadata = element.TryGetProperty("metadata", out var m) ? KubernetesJson.Deserialize<V1ObjectMeta>(m.GetRawText()) : null;
            return new UntypedKubernetesObject(apiVersion, kind, metadata, element.Clone());
        }

        public static UntypedKubernetesObject FromJson(string json)
        {
            using var document = JsonDocument.Parse(json ?? throw new ArgumentNullException(nameof(json)));
            return FromJson(document.RootElement);
        }

        public override string ToString() => $"{Kind} {Metadata?.NamespaceProperty}/{Metadata?.Name}";
    }
}
