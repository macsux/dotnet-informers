using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using AwesomeAssertions;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesJsonTests
    {
        // what a trimmed or AOT app gets: only source-generated metadata, no reflection fallback
        private static readonly JsonSerializerOptions Strict = BuildStrict();

        private static JsonSerializerOptions BuildStrict()
        {
            KubernetesJsonContext.Register();
            return InformerJson.Build(allowReflection: false);
        }

        private static V1Pod SamplePod() => new V1Pod
        {
            ApiVersion = "v1",
            Kind = "Pod",
            Metadata = new V1ObjectMeta
            {
                Name = "web",
                NamespaceProperty = "prod",
                CreationTimestamp = new DateTime(2026, 10, 30, 12, 0, 0, DateTimeKind.Utc),
                Labels = new Dictionary<string, string> { ["app"] = "web" },
            },
            Spec = new V1PodSpec
            {
                Containers = new List<V1Container>
                {
                    new V1Container
                    {
                        Name = "app",
                        Image = "nginx",
                        Resources = new V1ResourceRequirements { Limits = new Dictionary<string, ResourceQuantity> { ["cpu"] = new ResourceQuantity("100m") } },
                        LivenessProbe = new V1Probe { HttpGet = new V1HTTPGetAction { Port = 8080 } },
                    },
                },
            },
        };

        [Fact]
        public void BuiltInModels_SerializeWithoutReflection_LikeKubernetesJson()
        {
            var pod = SamplePod();
            var json = JsonSerializer.Serialize(pod, (System.Text.Json.Serialization.Metadata.JsonTypeInfo<V1Pod>)Strict.GetTypeInfo(typeof(V1Pod)));
            json.Should().Be(KubernetesJson.Serialize(pod));
            var back = JsonSerializer.Deserialize(json, (System.Text.Json.Serialization.Metadata.JsonTypeInfo<V1Pod>)Strict.GetTypeInfo(typeof(V1Pod)));
            back.Spec.Containers[0].Resources.Limits["cpu"].ToString().Should().Be("100m");
            back.Spec.Containers[0].LivenessProbe.HttpGet.Port.Value.Should().Be("8080");
            back.Metadata.CreationTimestamp.Should().Be(pod.Metadata.CreationTimestamp);
        }

        [Fact]
        public void LibraryTypes_SerializeWithoutReflection()
        {
            Strict.TryGetTypeInfo(typeof(PartialObjectMetadata), out _).Should().BeTrue();
            var untyped = UntypedKubernetesObject.FromJson("{\"apiVersion\":\"example.com/v1\",\"kind\":\"Widget\",\"metadata\":{\"name\":\"w\"},\"spec\":{\"size\":3}}");
            untyped.Metadata.Labels = new Dictionary<string, string> { ["edited"] = "yes" };
            var json = JsonSerializer.Serialize(untyped, Strict.GetTypeInfo(typeof(UntypedKubernetesObject)));
            var back = UntypedKubernetesObject.FromJson(json);
            back.Kind.Should().Be("Widget");
            back.Metadata.Labels["edited"].Should().Be("yes", "metadata is written from the typed property");
            back.Get("spec")!.Value.GetProperty("size").GetInt32().Should().Be(3);
        }

        [Fact]
        public void UnknownType_WithoutReflection_IsNotResolved()
        {
            Strict.TryGetTypeInfo(typeof(UnregisteredResource), out _).Should().BeFalse();
        }

        [Fact]
        public void AppResolver_IsUsedForCustomTypes()
        {
            InformerJson.AddResolver(TestJsonContext.Default);
            var options = InformerJson.Build(allowReflection: false);
            options.TryGetTypeInfo(typeof(RegisteredResource), out var info).Should().BeTrue();
            JsonSerializer.Serialize(new RegisteredResource { Metadata = new V1ObjectMeta { Name = "r" }, Size = 2 }, info)
                .Should().Contain("\"size\":2").And.Contain("\"name\":\"r\"");
        }

        [Fact]
        public void GetTypeInfo_ExplainsHowToRegisterAMissingType()
        {
            var act = () => InformerJson.Build(allowReflection: false).GetTypeInfo(typeof(UnregisteredResource));
            act.Should().Throw<NotSupportedException>();
        }

        [Fact]
        public void DeepClone_CopiesThroughConfiguredMetadata()
        {
            var pod = SamplePod();
            var clone = pod.DeepClone();
            clone.Should().NotBeSameAs(pod);
            clone.Metadata.Labels.Should().NotBeSameAs(pod.Metadata.Labels);
            KubernetesJson.Serialize(clone).Should().Be(KubernetesJson.Serialize(pod));
        }

        [Fact]
        public void ErrorHelpers_DistinguishConflictFromAlreadyExists()
        {
            static Exception Error(int code, string reason) => new k8s.Autorest.HttpOperationException
            {
                Response = new k8s.Autorest.HttpResponseMessageWrapper(new System.Net.Http.HttpResponseMessage((System.Net.HttpStatusCode)code), $"{{\"kind\":\"Status\",\"reason\":\"{reason}\",\"code\":{code}}}"),
            };

            Error(409, "Conflict").IsConflict().Should().BeTrue();
            Error(409, "Conflict").IsAlreadyExists().Should().BeFalse();
            Error(409, "AlreadyExists").IsAlreadyExists().Should().BeTrue();
            Error(409, "AlreadyExists").IsConflict().Should().BeFalse();
            Error(404, "NotFound").IsNotFound().Should().BeTrue();
            Error(410, "Expired").IsGone().Should().BeTrue();
            new InvalidOperationException().IsNotFound().Should().BeFalse();
        }

        public sealed class UnregisteredResource : IKubernetesObject<V1ObjectMeta>
        {
            public string ApiVersion { get; set; }
            public string Kind { get; set; }
            public V1ObjectMeta Metadata { get; set; }
        }
    }

    [KubernetesEntity(Group = "example.com", ApiVersion = "v1", Kind = "Registered", PluralName = "registereds")]
    public sealed class RegisteredResource : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; } = "example.com/v1";
        public string Kind { get; set; } = "Registered";
        public V1ObjectMeta Metadata { get; set; }
        public int Size { get; set; }
    }

    [JsonSerializable(typeof(RegisteredResource))]
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    internal sealed partial class TestJsonContext : JsonSerializerContext;
}
