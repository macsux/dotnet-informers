using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Steeltoe.Informers.Operator.Web
{
    // The wire types of admission.k8s.io/v1 AdmissionReview and apiextensions.k8s.io/v1 ConversionReview (KubernetesClient doesn't model them).

    internal sealed class AdmissionReview
    {
        [JsonPropertyName("apiVersion")]
        public string ApiVersion { get; set; } = "admission.k8s.io/v1";

        [JsonPropertyName("kind")]
        public string Kind { get; set; } = "AdmissionReview";

        [JsonPropertyName("request")]
        public AdmissionRequest Request { get; set; }

        [JsonPropertyName("response")]
        public AdmissionResponse Response { get; set; }
    }

    internal sealed class AdmissionRequest
    {
        [JsonPropertyName("uid")]
        public string Uid { get; set; }

        [JsonPropertyName("kind")]
        public GroupVersionKind Kind { get; set; }

        [JsonPropertyName("resource")]
        public GroupVersionResourceDto Resource { get; set; }

        [JsonPropertyName("subResource")]
        public string SubResource { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("namespace")]
        public string Namespace { get; set; }

        [JsonPropertyName("operation")]
        public string Operation { get; set; }

        [JsonPropertyName("userInfo")]
        public UserInfo UserInfo { get; set; }

        [JsonPropertyName("object")]
        public JsonElement? Object { get; set; }

        [JsonPropertyName("oldObject")]
        public JsonElement? OldObject { get; set; }

        [JsonPropertyName("dryRun")]
        public bool? DryRun { get; set; }
    }

    internal sealed class GroupVersionKind
    {
        [JsonPropertyName("group")]
        public string Group { get; set; }

        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("kind")]
        public string Kind { get; set; }
    }

    internal sealed class GroupVersionResourceDto
    {
        [JsonPropertyName("group")]
        public string Group { get; set; }

        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("resource")]
        public string Resource { get; set; }
    }

    /// <summary>Who made an admission request.</summary>
    public sealed class UserInfo
    {
        [JsonPropertyName("username")]
        public string Username { get; set; }

        [JsonPropertyName("uid")]
        public string Uid { get; set; }

        [JsonPropertyName("groups")]
        public List<string> Groups { get; set; }
    }

    internal sealed class AdmissionResponse
    {
        [JsonPropertyName("uid")]
        public string Uid { get; set; }

        [JsonPropertyName("allowed")]
        public bool Allowed { get; set; }

        [JsonPropertyName("status")]
        public ReviewStatus Status { get; set; }

        [JsonPropertyName("warnings")]
        public List<string> Warnings { get; set; }

        [JsonPropertyName("patchType")]
        public string PatchType { get; set; }

        /// <summary>Base64 of the JSON patch.</summary>
        [JsonPropertyName("patch")]
        public string Patch { get; set; }
    }

    internal sealed class ReviewStatus
    {
        [JsonPropertyName("status")]
        public string Status { get; set; }

        [JsonPropertyName("code")]
        public int? Code { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }
    }

    internal sealed class ConversionReview
    {
        [JsonPropertyName("apiVersion")]
        public string ApiVersion { get; set; } = "apiextensions.k8s.io/v1";

        [JsonPropertyName("kind")]
        public string Kind { get; set; } = "ConversionReview";

        [JsonPropertyName("request")]
        public ConversionRequest Request { get; set; }

        [JsonPropertyName("response")]
        public ConversionResponse Response { get; set; }
    }

    internal sealed class ConversionRequest
    {
        [JsonPropertyName("uid")]
        public string Uid { get; set; }

        [JsonPropertyName("desiredAPIVersion")]
        public string DesiredApiVersion { get; set; }

        [JsonPropertyName("objects")]
        public List<JsonElement> Objects { get; set; }
    }

    internal sealed class ConversionResponse
    {
        [JsonPropertyName("uid")]
        public string Uid { get; set; }

        [JsonPropertyName("convertedObjects")]
        public List<JsonElement> ConvertedObjects { get; set; }

        [JsonPropertyName("result")]
        public ReviewStatus Result { get; set; }
    }

    [JsonSerializable(typeof(AdmissionReview))]
    [JsonSerializable(typeof(ConversionReview))]
    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    internal sealed partial class WebhookJsonContext : JsonSerializerContext;
}
