using System.Text.Json.Serialization;

namespace Steeltoe.Informers.Samples.Controllers
{
    /// <summary>Source-generated JSON metadata for the samples' custom resources (needed when the app is trimmed or published as NativeAOT).</summary>
    [JsonSerializable(typeof(AppSet))]
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    public sealed partial class SampleJsonContext : JsonSerializerContext;
}
