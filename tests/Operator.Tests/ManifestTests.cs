using System;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;

namespace Steeltoe.Informers.Operator.Tests
{
    public class ManifestTests
    {
        [Fact]
        public async Task Rbac_FromControllerRegistrations_WithoutACluster()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(new ControllerStats());
            OperatorOptions options = null;
            services.AddKubernetesOperator(o => options = o.LeaderElection("lease", "operators", "a"))
                .AddController<AppSetController>()
                .AddController<V1ConfigMap>(c => c.References<V1Secret>(m => new[] { "s" }).Reconcile((_, _) => Task.FromResult(ReconcileResult.Done)));
            await using var provider = services.BuildServiceProvider();

            var yaml = RbacManifest.Build(provider.GetRequiredService<OperatorStatus>(), options, "appset-operator");

            yaml.Should().Contain("kind: ClusterRole").And.Contain("name: appset-operator");
            yaml.Should().Contain("""
                - apiGroups: ["samples.steeltoe.io"]
                  resources: ["appsets"]
                  verbs: ["get", "list", "patch", "update", "watch"]
                """.Replace("\r", ""));
            yaml.Should().Contain("resources: [\"appsets/status\"]").And.Contain("resources: [\"appsets/finalizers\"]");
            yaml.Should().Contain("""
                - apiGroups: [""]
                  resources: ["pods"]
                  verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
                """.Replace("\r", ""), "the AppSet controller owns pods");
            yaml.Should().Contain("resources: [\"secrets\"]");
            yaml.Should().Contain("apiGroups: [\"events.k8s.io\"]").And.Contain("apiGroups: [\"coordination.k8s.io\"]");
        }
    }
}
