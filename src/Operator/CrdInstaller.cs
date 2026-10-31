using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>Development-time installation of CRDs embedded as YAML resources (see <see cref="OperatorBuilder.InstallCrds" />).</summary>
    internal static class CrdInstaller
    {
        /// <summary>A new CRD is served once the API server reports it <c>Established</c>; informers started earlier would fail discovery.</summary>
        private static async Task WaitUntilEstablishedAsync(IKubernetesGenericClient client, string name, CancellationToken cancellationToken)
        {
            for (var i = 0; i < 60; i++)
            {
                var crd = await client.GetAsync<V1CustomResourceDefinition>(name, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (crd?.Status?.Conditions?.Any(x => x.Type == "Established" && x.Status == "True") == true)
                    return;
                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
            }
        }

        public static async Task InstallAsync(IKubernetesGenericClient client, Assembly assembly, bool overwrite, ILogger logger, CancellationToken cancellationToken)
        {
            foreach (var name in assembly.GetManifestResourceNames().Where(x => x.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) || x.EndsWith(".yml", StringComparison.OrdinalIgnoreCase)))
            {
                await using var stream = assembly.GetManifestResourceStream(name);
                using var reader = new StreamReader(stream!);
                var yaml = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
                if (!yaml.Contains("kind: CustomResourceDefinition", StringComparison.Ordinal))
                    continue;
                var crd = KubernetesYaml.Deserialize<V1CustomResourceDefinition>(yaml);
                var existing = await client.GetAsync<V1CustomResourceDefinition>(crd.Metadata.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (existing == null)
                {
                    await client.CreateAsync(crd, cancellationToken: cancellationToken).ConfigureAwait(false);
                    logger.LogInformation("Installed CRD {Name}", crd.Metadata.Name);
                    await WaitUntilEstablishedAsync(client, crd.Metadata.Name, cancellationToken).ConfigureAwait(false);
                }
                else if (overwrite)
                {
                    crd.Metadata.ResourceVersion = existing.Metadata.ResourceVersion;
                    await client.ReplaceAsync(crd, cancellationToken: cancellationToken).ConfigureAwait(false);
                    logger.LogInformation("Replaced CRD {Name}", crd.Metadata.Name);
                }
            }
        }
    }
}
