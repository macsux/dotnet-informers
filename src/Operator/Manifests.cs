using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>The RBAC rules the operator's controllers need, as a <c>ClusterRole</c> manifest.</summary>
    public static class RbacManifest
    {
        /// <summary>The environment variable that starts the app in manifest mode: it writes its manifests there and exits.</summary>
        public const string OutputVariable = "INFORMERS_MANIFESTS_OUTPUT";

        /// <summary>
        ///     The <c>ClusterRole</c> YAML: the controllers' resources and verbs, plus Events (unless disabled) and Leases (with leader election).
        /// </summary>
        public static string Build(OperatorStatus status, OperatorOptions options, string name)
        {
            var rules = status.Resources.ToList();
            if (options.EventsEnabled)
            {
                rules.Add((new GroupVersionResource("events.k8s.io", "v1", "events"), new[] { "create", "patch" }));
                rules.Add((new GroupVersionResource(string.Empty, "v1", "events"), new[] { "create", "patch" }));
            }

            if (options.Leader != null)
                rules.Add((new GroupVersionResource("coordination.k8s.io", "v1", "leases"), new[] { "get", "create", "update" }));

            var sb = new StringBuilder();
            sb.Append("# generated from the operator's controller registrations by Steeltoe.Informers.Operator; don't edit\n");
            sb.Append("apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: ").Append(name).Append("\nrules:\n");
            // one rule per API group and verb set, resources sorted, so the output is stable
            foreach (var group in rules
                         .GroupBy(x => (x.Resource.Group, Verbs: string.Join(",", x.Verbs.Distinct().OrderBy(v => v, StringComparer.Ordinal))))
                         .OrderBy(x => x.Key.Group, StringComparer.Ordinal).ThenBy(x => x.Key.Verbs, StringComparer.Ordinal))
            {
                sb.Append("- apiGroups: [\"").Append(group.Key.Group).Append("\"]\n");
                sb.Append("  resources: [").Append(string.Join(", ", group.Select(x => $"\"{x.Resource.Resource}\"").Distinct().OrderBy(x => x, StringComparer.Ordinal))).Append("]\n");
                sb.Append("  verbs: [").Append(string.Join(", ", group.Key.Verbs.Split(',').Select(v => $"\"{v}\""))).Append("]\n");
            }

            return sb.ToString();
        }
    }

    /// <summary>
    ///     In manifest mode (<see cref="RbacManifest.OutputVariable" /> set, e.g. by the <c>InformersGenerateManifests</c> build target), writes
    ///     <c>rbac/role.yaml</c> from the controller registrations and stops the app before anything connects to a cluster.
    /// </summary>
    internal sealed class ManifestWriterService : IHostedService
    {
        private readonly OperatorStatus _status;
        private readonly OperatorOptions _options;
        private readonly IHostApplicationLifetime _lifetime;
        private readonly ILogger _logger;

        public ManifestWriterService(OperatorStatus status, OperatorOptions options, IHostApplicationLifetime lifetime = null, ILoggerFactory loggerFactory = null)
        {
            _status = status;
            _options = options;
            _lifetime = lifetime;
            _logger = loggerFactory?.CreateLogger("Steeltoe.Informers.Operator") ?? NullLogger.Instance;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (_options.ManifestOutput == null)
                return Task.CompletedTask;
            var file = Path.Combine(_options.ManifestOutput, "rbac", "role.yaml");
            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            var yaml = RbacManifest.Build(_status, _options, _options.RbacRoleName);
            if (!File.Exists(file) || File.ReadAllText(file) != yaml)
                File.WriteAllText(file, yaml);
            _logger.LogInformation("Wrote {File}", file);
            _lifetime?.StopApplication();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
