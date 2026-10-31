using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Source-generated JSON metadata for the types this library defines itself. Built-in Kubernetes models come from KubernetesClient's own
    ///     source-generated <c>k8s.SourceGenerationContext</c>; both are registered with <see cref="InformerJson" /> when this assembly loads.
    /// </summary>
    [JsonSerializable(typeof(PartialObjectMetadata))]
    [JsonSerializable(typeof(UntypedKubernetesObject))]
    [JsonSerializable(typeof(V1ObjectMeta))]
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    internal sealed partial class KubernetesJsonContext : JsonSerializerContext
    {
        [ModuleInitializer]
        [SuppressMessage("Usage", "CA2255", Justification = "Registers the library's JSON metadata before any informer serializes")]
        internal static void Register()
        {
            InformerJson.AddDefaultResolver(Default, BaseOptions);
            InformerJson.AddDefaultResolver(k8s.SourceGenerationContext.Default);
        }

        /// <summary>KubernetesClient's settings: camelCase, nulls omitted, and its date/time converters.</summary>
        private static JsonSerializerOptions BaseOptions()
        {
            var options = new JsonSerializerOptions(k8s.SourceGenerationContext.Default.Options) { TypeInfoResolver = null };
            return options;
        }
    }
}
