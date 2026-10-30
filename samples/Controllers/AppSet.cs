using System.Text.Json.Serialization;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>A custom resource that keeps <see cref="AppSetSpec.Replicas" /> pods running (see <c>Crds/appsets.yaml</c>).</summary>
    [KubernetesEntity(Group = "samples.steeltoe.io", ApiVersion = "v1", Kind = "AppSet", PluralName = "appsets")]
    public class AppSet : IKubernetesObject<V1ObjectMeta>, ISpec<AppSetSpec>, IStatus<AppSetStatus>
    {
        public const string Finalizer = "samples.steeltoe.io/pods";
        public const string OwnerLabel = "samples.steeltoe.io/appset";

        [JsonPropertyName("apiVersion")]
        public string ApiVersion { get; set; } = "samples.steeltoe.io/v1";

        [JsonPropertyName("kind")]
        public string Kind { get; set; } = "AppSet";

        [JsonPropertyName("metadata")]
        public V1ObjectMeta Metadata { get; set; }

        [JsonPropertyName("spec")]
        public AppSetSpec Spec { get; set; }

        [JsonPropertyName("status")]
        public AppSetStatus Status { get; set; }
    }

    public class AppSetSpec
    {
        [JsonPropertyName("replicas")]
        public int Replicas { get; set; }

        [JsonPropertyName("image")]
        public string Image { get; set; }
    }

    public class AppSetStatus
    {
        [JsonPropertyName("replicas")]
        public int Replicas { get; set; }

        [JsonPropertyName("readyReplicas")]
        public int ReadyReplicas { get; set; }

        [JsonPropertyName("observedGeneration")]
        public long? ObservedGeneration { get; set; }
    }
}
