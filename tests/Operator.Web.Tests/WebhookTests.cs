using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using k8s.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;

namespace Steeltoe.Informers.Operator.Web.Tests
{
    public sealed class AppSetValidator : IValidator<AppSet>
    {
        public IReadOnlyList<string> Operations => new[] { "CREATE", "UPDATE", "DELETE" };

        public ValueTask<AdmissionResult> CreateAsync(AppSet set, AdmissionContext context, CancellationToken ct) =>
            new(set.Spec.Replicas > 50 ? AdmissionResult.Deny("replicas must be at most 50") : AdmissionResult.Allow());

        public ValueTask<AdmissionResult> UpdateAsync(AppSet old, AppSet set, AdmissionContext context, CancellationToken ct) =>
            new(old.Spec.Image != set.Spec.Image ? AdmissionResult.Allow().WithWarning("changing the image restarts every pod") : AdmissionResult.Allow());

        public ValueTask<AdmissionResult> DeleteAsync(AppSet set, AdmissionContext context, CancellationToken ct) =>
            new(context.UserInfo?.Username == "intern" ? AdmissionResult.Deny("not you", 401) : AdmissionResult.Allow());
    }

    public sealed class AppSetDefaulter : IMutator<AppSet>
    {
        public ValueTask<AppSet> CreateAsync(AppSet set, AdmissionContext context, CancellationToken ct)
        {
            set.Spec.Image ??= "registry.k8s.io/pause:3.10";
            (set.Metadata.Labels ??= new Dictionary<string, string>())["defaulted/by"] = "webhook";
            return new(set);
        }
    }

    [KubernetesEntity(Group = "samples.steeltoe.io", ApiVersion = "v2", Kind = "AppSet", PluralName = "appsets")]
    public sealed class AppSetV2 : IKubernetesObject<V1ObjectMeta>
    {
        public string ApiVersion { get; set; } = "samples.steeltoe.io/v2";
        public string Kind { get; set; } = "AppSet";
        public V1ObjectMeta Metadata { get; set; }
        public V2Spec Spec { get; set; }

        public sealed class V2Spec
        {
            public int Count { get; set; }
            public List<string> Images { get; set; }
        }
    }

    /// <summary>v2 is the hub (storage) version, v1 a spoke.</summary>
    public sealed class AppSetV1Converter : IConverter<AppSet, AppSetV2>
    {
        public AppSetV2 ToHub(AppSet v1) => new AppSetV2
        {
            Metadata = v1.Metadata,
            Spec = new AppSetV2.V2Spec { Count = v1.Spec.Replicas, Images = v1.Spec.Image == null ? new List<string>() : new List<string> { v1.Spec.Image } },
        };

        public AppSet FromHub(AppSetV2 v2) => new AppSet
        {
            Metadata = v2.Metadata,
            Spec = new AppSetSpec { Replicas = v2.Spec.Count, Image = v2.Spec.Images?.FirstOrDefault() },
        };
    }

    public class WebhookTests : IAsyncLifetime
    {
        private WebApplication _app;
        private HttpClient _http;

        public async ValueTask InitializeAsync()
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.UseTestServer();
            builder.Services.AddKubernetesOperator(o => o.DisableEvents())
                .AddValidator<AppSetValidator>()
                .AddMutator<AppSetDefaulter>()
                .AddConverter<AppSetV1Converter>()
                .Webhooks(w =>
                {
                    w.ServiceName = "appset-operator";
                    w.ServiceNamespace = "operators";
                });
            // only the web part is under test
            foreach (var hosted in builder.Services.Where(x => x.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)).ToList())
                builder.Services.Remove(hosted);
            _app = builder.Build();
            _app.MapKubernetesWebhooks();
            await _app.StartAsync(TestContext.Current.CancellationToken);
            _http = _app.GetTestClient();
        }

        public async ValueTask DisposeAsync()
        {
            _http.Dispose();
            await _app.DisposeAsync();
        }

        private static string AppSetJson(int replicas, string image = null, string apiVersion = "samples.steeltoe.io/v1") =>
            new JsonObject
            {
                ["apiVersion"] = apiVersion,
                ["kind"] = "AppSet",
                ["metadata"] = new JsonObject { ["name"] = "web", ["namespace"] = "demo" },
                ["spec"] = image == null ? new JsonObject { ["replicas"] = replicas } : new JsonObject { ["replicas"] = replicas, ["image"] = image },
            }.ToJsonString();

        private async Task<JsonNode> PostAsync(string path, string body)
        {
            using var response = await _http.PostAsync(path, new StringContent(body, Encoding.UTF8, "application/json"), TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();
            return JsonNode.Parse(await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        }

        private Task<JsonNode> AdmitAsync(string path, string operation, string obj, string oldObj = null, string user = "admin") =>
            PostAsync(path, $$$"""
                {"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"req-1","operation":"{{{operation}}}","namespace":"demo","name":"web",
                "userInfo":{"username":"{{{user}}}"},"object":{{{obj ?? "null"}}},"oldObject":{{{oldObj ?? "null"}}}}}
                """);

        [Fact]
        public async Task Validating_AllowsDeniesAndWarns()
        {
            var allowed = await AdmitAsync("/validate/samples.steeltoe.io/v1/appsets", "CREATE", AppSetJson(3));
            allowed["response"]!["uid"]!.GetValue<string>().Should().Be("req-1");
            allowed["response"]!["allowed"]!.GetValue<bool>().Should().BeTrue();

            var denied = await AdmitAsync("/validate/samples.steeltoe.io/v1/appsets", "CREATE", AppSetJson(80));
            denied["response"]!["allowed"]!.GetValue<bool>().Should().BeFalse();
            denied["response"]!["status"]!["message"]!.GetValue<string>().Should().Be("replicas must be at most 50");
            denied["response"]!["status"]!["code"]!.GetValue<int>().Should().Be(403);

            var warned = await AdmitAsync("/validate/samples.steeltoe.io/v1/appsets", "UPDATE", AppSetJson(3, "nginx:2"), AppSetJson(3, "nginx:1"));
            warned["response"]!["allowed"]!.GetValue<bool>().Should().BeTrue();
            warned["response"]!["warnings"]!.AsArray().Single()!.GetValue<string>().Should().Contain("restarts");

            var delete = await AdmitAsync("/validate/samples.steeltoe.io/v1/appsets", "DELETE", null, AppSetJson(3), user: "intern");
            delete["response"]!["status"]!["code"]!.GetValue<int>().Should().Be(401);
        }

        [Fact]
        public async Task Mutating_ReturnsAJsonPatchOfTheChanges()
        {
            var review = await AdmitAsync("/mutate/samples.steeltoe.io/v1/appsets", "CREATE", AppSetJson(3));
            var response = review["response"]!;
            response["allowed"]!.GetValue<bool>().Should().BeTrue();
            response["patchType"]!.GetValue<string>().Should().Be("JSONPatch");
            var patch = JsonNode.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(response["patch"]!.GetValue<string>())))!.AsArray();
            patch.Select(x => (x!["op"]!.GetValue<string>(), x["path"]!.GetValue<string>())).Should().BeEquivalentTo(new[]
            {
                ("add", "/metadata/labels"),
                ("add", "/spec/image"),
            });

            var unchanged = await AdmitAsync("/mutate/samples.steeltoe.io/v1/appsets", "UPDATE", AppSetJson(3, "nginx"), AppSetJson(2, "nginx"));
            unchanged["response"]!["patch"].Should().BeNull("the default update changes nothing");
        }

        [Fact]
        public async Task Conversion_HubAndSpoke_BothDirections()
        {
            var toHub = await PostAsync("/convert/samples.steeltoe.io/appsets", $$$"""
                {"apiVersion":"apiextensions.k8s.io/v1","kind":"ConversionReview","request":{"uid":"c-1","desiredAPIVersion":"samples.steeltoe.io/v2","objects":[{{{AppSetJson(4, "nginx")}}}]}}
                """);
            var response = toHub["response"]!;
            response["uid"]!.GetValue<string>().Should().Be("c-1");
            response["result"]!["status"]!.GetValue<string>().Should().Be("Success");
            var v2 = response["convertedObjects"]!.AsArray().Single()!;
            v2["apiVersion"]!.GetValue<string>().Should().Be("samples.steeltoe.io/v2");
            v2["spec"]!["count"]!.GetValue<int>().Should().Be(4);
            v2["metadata"]!["name"]!.GetValue<string>().Should().Be("web");

            var fromHub = await PostAsync("/convert/samples.steeltoe.io/appsets", $$$"""
                {"apiVersion":"apiextensions.k8s.io/v1","kind":"ConversionReview","request":{"uid":"c-2","desiredAPIVersion":"samples.steeltoe.io/v1","objects":[{{{v2.ToJsonString()}}}]}}
                """);
            var v1 = fromHub["response"]!["convertedObjects"]!.AsArray().Single()!;
            v1["apiVersion"]!.GetValue<string>().Should().Be("samples.steeltoe.io/v1");
            v1["spec"]!["replicas"]!.GetValue<int>().Should().Be(4);
            v1["spec"]!["image"]!.GetValue<string>().Should().Be("nginx");

            var unknown = await PostAsync("/convert/samples.steeltoe.io/appsets", """
                {"request":{"uid":"c-3","desiredAPIVersion":"samples.steeltoe.io/v9","objects":[{"apiVersion":"samples.steeltoe.io/v1","kind":"AppSet","metadata":{"name":"x"}}]}}
                """);
            unknown["response"]!["result"]!["status"]!.GetValue<string>().Should().Be("Failure");
        }

        [Fact]
        public void Manifests_WebhookConfigurationsAndCrdConversion()
        {
            var files = _app.Services.GetServices<IOperatorManifest>().SelectMany(x => x.Generate()).ToDictionary(x => x.Path, x => x.Content);
            files.Keys.Should().BeEquivalentTo("webhooks/validating.yaml", "webhooks/mutating.yaml", "webhooks/conversion.yaml");
            var validating = KubernetesYaml.Deserialize<V1ValidatingWebhookConfiguration>(files["webhooks/validating.yaml"]);
            validating.Metadata.Name.Should().Be("appset-operator-validating");
            var webhook = validating.Webhooks.Single();
            webhook.ClientConfig.Service.Should().BeEquivalentTo(new Admissionregistrationv1ServiceReference { Name = "appset-operator", NamespaceProperty = "operators", Port = 443, Path = "/validate/samples.steeltoe.io/v1/appsets" });
            webhook.Rules.Single().Operations.Should().Equal("CREATE", "UPDATE", "DELETE");
            webhook.SideEffects.Should().Be("None");
            KubernetesYaml.Deserialize<V1MutatingWebhookConfiguration>(files["webhooks/mutating.yaml"]).Webhooks.Single().ClientConfig.Service.Path.Should().Be("/mutate/samples.steeltoe.io/v1/appsets");
            var conversion = KubernetesYaml.LoadAllFromString(files["webhooks/conversion.yaml"]).Cast<V1CustomResourceDefinition>().Single();
            conversion.Metadata.Name.Should().Be("appsets.samples.steeltoe.io");
            conversion.Spec.Conversion.Strategy.Should().Be("Webhook");
            conversion.Spec.Conversion.Webhook.ClientConfig.Service.Path.Should().Be("/convert/samples.steeltoe.io/appsets");
        }
    }
}
