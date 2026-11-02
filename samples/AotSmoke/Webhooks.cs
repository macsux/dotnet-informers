using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Steeltoe.Informers.Operator;
using Steeltoe.Informers.Operator.Web;

namespace Steeltoe.Informers.Samples.AotSmoke
{
    public sealed class ConfigMapValidator : IValidator<V1ConfigMap>
    {
        public ValueTask<AdmissionResult> CreateAsync(V1ConfigMap obj, AdmissionContext context, CancellationToken cancellationToken) =>
            new(obj.Data?.ContainsKey("forbidden") == true ? AdmissionResult.Deny("forbidden key") : AdmissionResult.Allow());
    }

    public sealed class ConfigMapDefaulter : IMutator<V1ConfigMap>
    {
        public ValueTask<V1ConfigMap> CreateAsync(V1ConfigMap obj, AdmissionContext context, CancellationToken cancellationToken)
        {
            (obj.Metadata.Labels ??= new Dictionary<string, string>())["defaulted"] = "true";
            return new(obj);
        }
    }

    /// <summary>The webhook endpoints (minimal APIs, source-generated JSON) served by Kestrel in the NativeAOT app, called over HTTP.</summary>
    internal static class Webhooks
    {
        public static async Task CheckAsync(Program.Checks checks, CancellationToken ct)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Services.AddKubernetesOperator(o => o.DisableEvents()).AddValidator<ConfigMapValidator>().AddMutator<ConfigMapDefaulter>();
            foreach (var hosted in builder.Services.Where(x => x.ServiceType == typeof(IHostedService) && x.ImplementationType?.Namespace?.StartsWith("Steeltoe") == true).ToList())
                builder.Services.Remove(hosted);
            await using var app = builder.Build();
            app.MapKubernetesWebhooks();
            await app.StartAsync(ct);
            var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
            using var http = new HttpClient { BaseAddress = new Uri(address) };

            static string Review(string data) =>
                "{\"request\":{\"uid\":\"u\",\"operation\":\"CREATE\",\"object\":{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":{\"name\":\"c\"},\"data\":" + data + "}}}";

            async Task<JsonNode> Post(string path, string body)
            {
                using var response = await http.PostAsync(path, new StringContent(body, Encoding.UTF8, "application/json"), ct);
                return JsonNode.Parse(await response.Content.ReadAsStringAsync(ct))!["response"];
            }

            checks.Check("validating webhook allows", (await Post("/validate/core/v1/configmaps", Review("{\"k\":\"v\"}")))!["allowed"]!.GetValue<bool>());
            checks.Check("validating webhook denies", !(await Post("/validate/core/v1/configmaps", Review("{\"forbidden\":\"v\"}")))!["allowed"]!.GetValue<bool>());
            var mutated = await Post("/mutate/core/v1/configmaps", Review("{\"k\":\"v\"}"));
            var patch = Encoding.UTF8.GetString(Convert.FromBase64String(mutated!["patch"]!.GetValue<string>()));
            checks.Check("mutating webhook returns a JSON patch", patch.Contains("\"/metadata/labels\"", StringComparison.Ordinal));
            await app.StopAsync(ct);
        }
    }
}
