using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.Operator.Tests
{
    public class HostingTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        public sealed class Seen
        {
            public ConcurrentDictionary<string, int> Reconciles { get; } = new ConcurrentDictionary<string, int>();
        }

        public sealed class ScopedDependency
        {
            public Guid Id { get; } = Guid.NewGuid();
        }

        public sealed class MapController(Seen seen, ScopedDependency scoped) : Controller<V1ConfigMap>
        {
            public static readonly ConcurrentBag<Guid> Scopes = new ConcurrentBag<Guid>();

            public override void Configure(ControllerBuilder<V1ConfigMap> controller) => controller.Owns<V1Pod>().References<V1Secret>(m => new[] { "s" });

            public override Task<ReconcileResult> ReconcileAsync(ReconcileContext<V1ConfigMap> context, CancellationToken cancellationToken)
            {
                Scopes.Add(scoped.Id);
                seen.Reconciles.AddOrUpdate(context.Key, 1, (_, n) => n + 1);
                return Task.FromResult(ReconcileResult.Done);
            }
        }

        [Fact]
        public async Task Operator_RunsClassAndDelegateControllers_WithAScopePerReconcile()
        {
            var api = new FakeApiServer();
            api.Put(GroupVersionResource.For<V1ConfigMap>(), new V1ConfigMap { Metadata = new V1ObjectMeta { Name = "a", NamespaceProperty = "demo", Uid = "a" } });
            api.Put(GroupVersionResource.For<V1ConfigMap>(), new V1ConfigMap { Metadata = new V1ObjectMeta { Name = "b", NamespaceProperty = "demo", Uid = "b" } });
            api.Put(GroupVersionResource.For<V1Secret>(), new V1Secret { Metadata = new V1ObjectMeta { Name = "s", NamespaceProperty = "demo" } });
            var seen = new Seen();
            var delegateSeen = new ConcurrentBag<string>();

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IKubernetesInformerFactory>(new KubernetesInformerFactory(api, retryPolicy: new RetryPolicy((_, _) => true, _ => TimeSpan.FromMilliseconds(10))));
            services.AddSingleton<IKubernetesGenericClient>(new TestKubernetesClient());
            services.AddSingleton(seen);
            services.AddScoped<ScopedDependency>();
            services.AddKubernetesOperator(o => o.Namespace("demo").DisableEvents())
                .AddController<MapController>()
                .AddController<V1Secret>(c => c.Named("secrets").Reconcile((ctx, _) =>
                {
                    delegateSeen.Add(ctx.Key);
                    return Task.FromResult(ReconcileResult.Done);
                }));
            await using var provider = services.BuildServiceProvider();
            var hosted = provider.GetServices<IHostedService>().Single();
            await hosted.StartAsync(Ct);
            try
            {
                var status = provider.GetRequiredService<OperatorStatus>();
                status.IsSynced.Should().BeTrue("startup waits for the controllers' informers");
                status.Controllers.Should().BeEquivalentTo("MapController", "secrets");
                await Eventually(() => seen.Reconciles.Count == 2 && delegateSeen.Count == 1);
                MapController.Scopes.Distinct().Count().Should().BeGreaterThanOrEqualTo(2, "every reconcile gets its own scope");
                var resources = status.Resources.ToDictionary(x => x.Resource.Resource, x => x.Verbs);
                resources["configmaps"].Should().Contain(new[] { "get", "list", "watch", "patch" });
                resources["pods"].Should().Contain(new[] { "create", "delete" });
                resources["secrets"].Should().Contain("watch").And.Contain("patch", "the delegate controller's primary");
            }
            finally
            {
                await hosted.StopAsync(Ct);
            }
        }

        private static async Task Eventually(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (!condition() && DateTime.UtcNow < deadline)
                await Task.Delay(20, Ct);
            condition().Should().BeTrue();
        }
    }
}
