using System.Text.Json.Serialization;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     An object as served by the API server's metadata-only representation (<c>meta.k8s.io/v1 PartialObjectMetadata</c>): its
    ///     <c>apiVersion</c>, <c>kind</c> and <c>metadata</c> (labels, annotations, owner references, uid, generation, deletion timestamp,
    ///     finalizers), without <c>spec</c> or <c>status</c>.
    /// </summary>
    public class PartialObjectMetadata : IKubernetesObject<V1ObjectMeta>
    {
        [JsonPropertyName("apiVersion")]
        public string ApiVersion { get; set; }

        [JsonPropertyName("kind")]
        public string Kind { get; set; }

        [JsonPropertyName("metadata")]
        public V1ObjectMeta Metadata { get; set; }

        public override string ToString() => $"{Kind} {Metadata?.NamespaceProperty}/{Metadata?.Name}";
    }
}
