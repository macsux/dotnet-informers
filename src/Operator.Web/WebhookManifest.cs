using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.Operator.Web
{
    /// <summary>
    ///     R7.5: in manifest mode, writes <c>webhooks/validating.yaml</c>, <c>webhooks/mutating.yaml</c> and <c>webhooks/conversion.yaml</c> (a merge
    ///     patch per CRD setting <c>spec.conversion</c>) from the registered webhooks.
    /// </summary>
    internal sealed class WebhookManifest : IOperatorManifest
    {
        private const string Header = "# generated from the operator's webhook registrations by Steeltoe.Informers.Operator.Web; don't edit\n";
        private readonly IServiceProvider _services;
        private readonly WebhookOptions _options;

        public WebhookManifest(IServiceProvider services, WebhookOptions options)
        {
            _services = services;
            _options = options;
        }

        public IEnumerable<(string Path, string Content)> Generate()
        {
            var descriptors = WebhookExtensions.Descriptors(_services);
            var name = _options.Name ?? _options.ServiceName;
            var validating = descriptors.Where(x => x.Kind == WebhookKind.Validating).ToList();
            if (validating.Count > 0)
            {
                var configuration = new V1ValidatingWebhookConfiguration
                {
                    ApiVersion = "admissionregistration.k8s.io/v1",
                    Kind = "ValidatingWebhookConfiguration",
                    Metadata = new V1ObjectMeta { Name = $"{name}-validating" },
                    Webhooks = validating.Select(x => new V1ValidatingWebhook
                    {
                        Name = WebhookName(x, "validate"),
                        AdmissionReviewVersions = new List<string> { "v1" },
                        SideEffects = "None",
                        FailurePolicy = _options.FailurePolicy,
                        TimeoutSeconds = _options.TimeoutSeconds,
                        ClientConfig = ClientConfig(x),
                        Rules = new List<V1RuleWithOperations> { Rule(x) },
                    }).ToList(),
                };
                yield return ("webhooks/validating.yaml", Header + ManifestYaml.Write(Patching(configuration)));
            }

            var mutating = descriptors.Where(x => x.Kind == WebhookKind.Mutating).ToList();
            if (mutating.Count > 0)
            {
                var configuration = new V1MutatingWebhookConfiguration
                {
                    ApiVersion = "admissionregistration.k8s.io/v1",
                    Kind = "MutatingWebhookConfiguration",
                    Metadata = new V1ObjectMeta { Name = $"{name}-mutating" },
                    Webhooks = mutating.Select(x => new V1MutatingWebhook
                    {
                        Name = WebhookName(x, "mutate"),
                        AdmissionReviewVersions = new List<string> { "v1" },
                        SideEffects = "None",
                        FailurePolicy = _options.FailurePolicy,
                        TimeoutSeconds = _options.TimeoutSeconds,
                        ReinvocationPolicy = "Never",
                        ClientConfig = ClientConfig(x),
                        Rules = new List<V1RuleWithOperations> { Rule(x) },
                    }).ToList(),
                };
                yield return ("webhooks/mutating.yaml", Header + ManifestYaml.Write(Patching(configuration)));
            }

            var conversions = descriptors.Where(x => x.Kind == WebhookKind.Converting).GroupBy(x => x.Path).ToList();
            if (conversions.Count > 0)
            {
                var sb = new StringBuilder(Header);
                sb.Append("# merge patches for the generated CRDs: kubectl patch crd <name> --type merge --patch-file <document>, or a kustomize patch\n");
                foreach (var group in conversions)
                {
                    var descriptor = group.First();
                    var patch = new JsonObject
                    {
                        ["apiVersion"] = "apiextensions.k8s.io/v1",
                        ["kind"] = "CustomResourceDefinition",
                        ["metadata"] = new JsonObject { ["name"] = $"{descriptor.Resource.Resource}.{descriptor.Resource.Group}" },
                        ["spec"] = new JsonObject
                        {
                            ["conversion"] = new JsonObject
                            {
                                ["strategy"] = "Webhook",
                                ["webhook"] = new JsonObject
                                {
                                    ["conversionReviewVersions"] = new JsonArray("v1"),
                                    ["clientConfig"] = ClientConfigJson(descriptor),
                                },
                            },
                        },
                    };
                    sb.Append("---\n").Append(ManifestYaml.Write(patch));
                }

                yield return ("webhooks/conversion.yaml", sb.ToString());
            }
        }

        private static JsonNode Patching(object model) => JsonNode.Parse(InformerJson.Serialize(model));

        private string WebhookName(WebhookDescriptor descriptor, string verb) =>
            $"{verb}.{descriptor.Resource.Resource}.{(string.IsNullOrEmpty(descriptor.Resource.Group) ? "core" : descriptor.Resource.Group)}";

        private Admissionregistrationv1WebhookClientConfig ClientConfig(WebhookDescriptor descriptor) => new Admissionregistrationv1WebhookClientConfig
        {
            Service = new Admissionregistrationv1ServiceReference { Name = _options.ServiceName, NamespaceProperty = _options.ServiceNamespace, Port = _options.Port, Path = descriptor.Path },
            CaBundle = _options.CaBundle == null ? null : Convert.FromBase64String(_options.CaBundle),
        };

        private JsonObject ClientConfigJson(WebhookDescriptor descriptor)
        {
            var config = new JsonObject
            {
                ["service"] = new JsonObject
                {
                    ["name"] = _options.ServiceName,
                    ["namespace"] = _options.ServiceNamespace,
                    ["port"] = _options.Port,
                    ["path"] = descriptor.Path,
                },
            };
            if (_options.CaBundle != null)
                config["caBundle"] = _options.CaBundle;
            return config;
        }

        private static V1RuleWithOperations Rule(WebhookDescriptor descriptor) => new V1RuleWithOperations
        {
            ApiGroups = new List<string> { descriptor.Resource.Group },
            ApiVersions = new List<string> { descriptor.Resource.Version },
            Resources = new List<string> { descriptor.Resource.Resource },
            Operations = descriptor.Operations.ToList(),
            Scope = "*",
        };
    }
}
