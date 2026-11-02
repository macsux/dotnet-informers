using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     A Kubernetes object without a C# type: typed standard metadata plus the raw JSON of the whole object. Read fields from
    ///     <see cref="Raw" /> (e.g. <c>Raw.GetProperty("spec")</c>); it's immutable, so it can be shared safely.
    ///     It serializes as the raw object, with <see cref="ApiVersion" />, <see cref="Kind" /> and <see cref="Metadata" /> taken from the properties.
    /// </summary>
    [JsonConverter(typeof(UntypedKubernetesObjectConverter))]
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
            var metadata = element.TryGetProperty("metadata", out var m) ? InformerJson.Deserialize<V1ObjectMeta>(m) : null;
            return new UntypedKubernetesObject(apiVersion, kind, metadata, element.Clone());
        }

        public static UntypedKubernetesObject FromJson(string json)
        {
            using var document = JsonDocument.Parse(json ?? throw new ArgumentNullException(nameof(json)));
            return FromJson(document.RootElement);
        }

        public override string ToString() => $"{Kind} {Metadata?.NamespaceProperty}/{Metadata?.Name}";
    }

    internal sealed class UntypedKubernetesObjectConverter : JsonConverter<UntypedKubernetesObject>
    {
        public override UntypedKubernetesObject Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            return UntypedKubernetesObject.FromJson(document.RootElement);
        }

        public override void Write(Utf8JsonWriter writer, UntypedKubernetesObject value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            if (value.ApiVersion != null)
                writer.WriteString("apiVersion", value.ApiVersion);
            if (value.Kind != null)
                writer.WriteString("kind", value.Kind);
            if (value.Metadata != null)
            {
                writer.WritePropertyName("metadata");
                JsonSerializer.Serialize(writer, value.Metadata, InformerJson.GetTypeInfo<V1ObjectMeta>());
            }

            if (value.Raw.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in value.Raw.EnumerateObject())
                {
                    if (property.NameEquals("apiVersion") || property.NameEquals("kind") || property.NameEquals("metadata"))
                        continue;
                    property.WriteTo(writer);
                }
            }

            writer.WriteEndObject();
        }
    }
}
