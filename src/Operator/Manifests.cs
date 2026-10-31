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
    /// <summary>A manifest the operator writes in manifest mode, next to its RBAC role (e.g. webhook configurations).</summary>
    public interface IOperatorManifest
    {
        /// <summary>The files to write: a path relative to the manifest output directory, and the content.</summary>
        IEnumerable<(string Path, string Content)> Generate();
    }

    /// <summary>Writes JSON (e.g. a Kubernetes model serialized through <c>InformerJson</c>) as block-style YAML, without reflection.</summary>
    public static class ManifestYaml
    {
        public static string Write(System.Text.Json.Nodes.JsonNode node)
        {
            var sb = new StringBuilder();
            WriteNode(sb, node, 0, inSequence: false);
            return sb.ToString();
        }

        private static void WriteNode(StringBuilder sb, System.Text.Json.Nodes.JsonNode node, int indent, bool inSequence)
        {
            switch (node)
            {
                case System.Text.Json.Nodes.JsonObject obj when obj.Count > 0:
                    var first = true;
                    foreach (var (key, value) in obj)
                    {
                        if (!(first && inSequence))
                            sb.Append(' ', indent);
                        first = false;
                        sb.Append(Scalar(key)).Append(':');
                        WriteValue(sb, value, indent);
                    }

                    break;
                case System.Text.Json.Nodes.JsonArray array when array.Count > 0:
                    foreach (var item in array)
                    {
                        sb.Append(' ', indent).Append("- ");
                        if (item is System.Text.Json.Nodes.JsonObject { Count: > 0 } || item is System.Text.Json.Nodes.JsonArray { Count: > 0 })
                            WriteNode(sb, item, indent + 2, inSequence: true);
                        else
                            sb.Append(Scalar(item)).Append('\n');
                    }

                    break;
                default:
                    sb.Append(' ', indent).Append(Scalar(node)).Append('\n');
                    break;
            }
        }

        private static void WriteValue(StringBuilder sb, System.Text.Json.Nodes.JsonNode value, int indent)
        {
            if (value is System.Text.Json.Nodes.JsonObject { Count: > 0 } || value is System.Text.Json.Nodes.JsonArray { Count: > 0 })
            {
                sb.Append('\n');
                WriteNode(sb, value, value is System.Text.Json.Nodes.JsonArray ? indent : indent + 2, inSequence: false);
            }
            else
            {
                sb.Append(' ').Append(Scalar(value)).Append('\n');
            }
        }

        // JSON scalars are valid YAML flow scalars; strings are always quoted, so nothing is misread (yes, 1.0, ~)
        private static string Scalar(System.Text.Json.Nodes.JsonNode node) => node switch
        {
            null => "null",
            System.Text.Json.Nodes.JsonObject => "{}",
            System.Text.Json.Nodes.JsonArray => "[]",
            _ => node.ToJsonString(),
        };

        private static string Scalar(string key) => System.Text.Json.Nodes.JsonValue.Create(key)!.ToJsonString();
    }

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
                rules.Add((new GroupVersionResource("coordination.k8s.io", "v1", "leases"), new[] { "get", "create", "update", "patch" }));

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
        private readonly IEnumerable<IOperatorManifest> _manifests;

        public ManifestWriterService(OperatorStatus status, OperatorOptions options, IEnumerable<IOperatorManifest> manifests, IHostApplicationLifetime lifetime = null, ILoggerFactory loggerFactory = null)
        {
            _status = status;
            _manifests = manifests;
            _options = options;
            _lifetime = lifetime;
            _logger = loggerFactory?.CreateLogger("Steeltoe.Informers.Operator") ?? NullLogger.Instance;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (_options.ManifestOutput == null)
                return Task.CompletedTask;
            Write(Path.Combine("rbac", "role.yaml"), RbacManifest.Build(_status, _options, _options.RbacRoleName));
            foreach (var manifest in _manifests)
            {
                foreach (var (path, content) in manifest.Generate())
                    Write(path, content);
            }

            _lifetime?.StopApplication();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private void Write(string relativePath, string content)
        {
            var file = Path.Combine(_options.ManifestOutput, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            if (!File.Exists(file) || File.ReadAllText(file) != content)
                File.WriteAllText(file, content);
            _logger.LogInformation("Wrote {File}", file);
        }
    }
}
