using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Cache;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;
using static Steeltoe.Informers.Integration.Tests.ClusterFixture;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>Informer behaviour against a real cluster: filter transitions, sharing, disruption and leader election.</summary>
    [Collection(ClusterCollection.Name)]
    public sealed class InformerScenarioTests : IAsyncLifetime
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(90);
        private readonly ClusterFixture _cluster;
        private readonly ITestOutputHelper _output;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly List<Task> _consumers = new List<Task>();
        private string _ns;

        public InformerScenarioTests(ClusterFixture cluster, ITestOutputHelper output)
        {
            _cluster = cluster;
            _output = output;
        }

        public async ValueTask InitializeAsync()
        {
            if (!string.IsNullOrEmpty(KubeconfigPath))
                _ns = await _cluster.CreateNamespaceAsync("inf");
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            foreach (var consumer in _consumers)
            {
                try
                {
                    await consumer.WaitAsync(TimeSpan.FromSeconds(30));
                }
                catch (Exception)
                {
                    // cancelled
                }
            }

            if (_ns != null)
                await _cluster.DeleteNamespaceAsync(_ns);
        }

        /// <summary>A consumer that records every event and mirrors the informable into a cache.</summary>
        private sealed class Recorder<T>
        {
            public SimpleCache<string, T> View { get; } = new SimpleCache<string, T>();
            public ConcurrentQueue<ResourceEvent<string, T>> Events { get; } = new ConcurrentQueue<ResourceEvent<string, T>>();
            public Task Completion { get; set; }
        }

        private Recorder<T> Record<T>(IInformable<string, T> source)
        {
            var recorder = new Recorder<T>();
            recorder.Completion = Task.Run(async () =>
            {
                await foreach (var e in source.Into(recorder.View).WithCancellation(_stop.Token))
                    recorder.Events.Enqueue(e);
            });
            _consumers.Add(recorder.Completion);
            return recorder;
        }

        private async Task<HashSet<string>> PodKeys(string labelSelector = null) =>
            (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns, labelSelector: labelSelector)).Items.Select(p => p.ObjectKey()).ToHashSet();

        private Task Label(string pod, string tier) =>
            _cluster.Admin.CoreV1.PatchNamespacedPodAsync(new V1Patch($"{{\"metadata\":{{\"labels\":{{\"tier\":{(tier == null ? "null" : $"\"{tier}\"")}}}}}}}", V1Patch.PatchType.MergePatch), pod, _ns);

        [Fact]
        public async Task ServerFilterTransitions_RelabeledIntoAndOutOfPushedDownSelector()
        {
            SkipIfNoCluster();
            var (client, requests) = _cluster.NewClient();
            using var _ = client;
            var informers = new KubernetesInformerFactory(client);
            foreach (var (name, tier) in new[] { ("a", "gold"), ("b", "silver"), ("c", null) })
                await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, name, tier == null ? null : new Dictionary<string, string> { ["tier"] = tier }), _ns);
            var gold = Record(informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == _ns && p.GetLabel("tier") == "gold"));

            async Task ViewMatchesCluster() => await Eventually(async () => gold.View.Keys.ToHashSet().SetEquals(await PodKeys("tier=gold")), Timeout,
                async () => $"view [{string.Join(",", gold.View.Keys)}] cluster [{string.Join(",", await PodKeys("tier=gold"))}]");

            await ViewMatchesCluster();
            await Label("b", "gold");   // enters the selector
            await Label("a", "silver"); // leaves it
            await Label("c", "gold");   // enters (label added)
            await ViewMatchesCluster();
            await Label("b", null);     // leaves (label removed)
            await ViewMatchesCluster();

            var after = gold.Events.SkipWhile(e => e.EventFlags.HasFlag(EventTypeFlags.Reset)).ToList();
            after.Where(e => e.Key == $"{_ns}/a").Select(e => e.EventFlags).Should().Contain(EventTypeFlags.Delete);
            after.Where(e => e.Key == $"{_ns}/c").Select(e => e.EventFlags).Should().Contain(EventTypeFlags.Add);
            var lists = requests.Requests.Where(x => x.Path == $"/api/v1/namespaces/{_ns}/pods").ToList();
            lists.Should().NotBeEmpty().And.OnlyContain(x => x.Query.Contains("labelSelector=tier=gold"));
        }

        [Fact]
        public async Task Sharing_SeveralConsumersOneWatch_TransformTakesEffect()
        {
            SkipIfNoCluster();
            var (client, requests) = _cluster.NewClient();
            using var _ = client;
            var informers = new KubernetesInformerFactory(client, new KubernetesInformerRegistration[] { new KubernetesInformerRegistration<V1Pod>().StripManagedFields() });
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "a", new Dictionary<string, string> { ["app"] = "web" }), _ns);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "b", new Dictionary<string, string> { ["app"] = "db" }), _ns);
            var pods = informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == _ns);
            var consumers = new[]
            {
                Record(pods),
                Record(pods.Where(p => p.Spec.Containers.Count == 1)),          // client-side remainder only: same producer
                Record(informers.Get<V1Pod>().Where(p => p.Namespace() == _ns)), // the same server filter, written differently
            };
            var path = $"/api/v1/namespaces/{_ns}/pods";
            await Eventually(() => consumers.All(c => c.View.Count == 2) && requests.Watches(path) == 1, Timeout, () => $"views {string.Join(",", consumers.Select(c => c.View.Count))} watches {requests.Watches(path)}");
            await Task.Delay(1000);
            requests.Lists(path).Should().Be(1);
            requests.Watches(path).Should().Be(1);
            requests.OpenWatches.Should().Be(1, "three consumers share one watch connection");

            consumers[0].View.Values.Should().OnlyContain(p => p.Metadata.ManagedFields == null, "the transform strips managedFields before caching");
            (await _cluster.Admin.CoreV1.ReadNamespacedPodAsync("a", _ns)).Metadata.ManagedFields.Should().NotBeNullOrEmpty("the API server does send them");
            ReferenceEquals(consumers[0].View[$"{_ns}/a"], consumers[2].View[$"{_ns}/a"]).Should().BeTrue("consumers share the same instances");
        }

        [Fact]
        public async Task DroppedConnections_ResumeWatch_ConsumersConverge()
        {
            SkipIfNoCluster();
            var server = new Uri(_cluster.Config.Host);
            using var proxy = new TcpProxy(server);
            var (client, requests) = _cluster.NewClient(proxy.Address(server));
            using var _ = client;
            var informers = new KubernetesInformerFactory(client);
            for (var i = 0; i < 3; i++)
                await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, $"p{i}"), _ns);
            var recorder = Record(informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == _ns));
            var path = $"/api/v1/namespaces/{_ns}/pods";
            await Eventually(async () => recorder.View.Keys.ToHashSet().SetEquals(await PodKeys()) && requests.OpenWatches == 1, Timeout);
            var eventsBefore = recorder.Events.Count;

            proxy.Cut();
            await _cluster.Admin.CoreV1.DeleteNamespacedPodAsync("p0", _ns, gracePeriodSeconds: 0);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "p3"), _ns);
            await Label("p1", "changed");
            await Task.Delay(2000);
            proxy.Restore();

            await Eventually(async () => recorder.View.Keys.ToHashSet().SetEquals(await PodKeys()) && recorder.View[$"{_ns}/p1"].Metadata.Labels?["tier"] == "changed", Timeout,
                async () => $"view [{string.Join(",", recorder.View.Keys)}] cluster [{string.Join(",", await PodKeys())}]");
            recorder.Events.Skip(eventsBefore).Should().NotContain(e => e.EventFlags.HasFlag(EventTypeFlags.Reset), "a reconnect never sends a reset");
            requests.Watches(path).Should().BeGreaterThan(1, "the watch was re-established");
            _output.WriteLine($"lists={requests.Lists(path)} watches={requests.Watches(path)}");
        }

        [Fact]
        public async Task WatchExpired_AfterEtcdCompactionAndApiServerRestart_RelistArrivesAsComputedChangesOnly()
        {
            SkipIfNoCluster();
            Assert.SkipWhen(string.IsNullOrEmpty(KindCluster), "INFORMERS_INTEGRATION_KIND_CLUSTER is not set");
            var server = new Uri(_cluster.Config.Host);
            using var proxy = new TcpProxy(server);
            var (client, requests) = _cluster.NewClient(proxy.Address(server));
            using var _ = client;
            var informers = new KubernetesInformerFactory(client);
            for (var i = 0; i < 4; i++)
                await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, $"p{i}"), _ns);
            // pods settle first (Running), so the ones not touched below really are unchanged
            await Eventually(async () => (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns)).Items.All(p => p.Status?.Phase == "Running" && p.Status.Conditions?.Any(c => c.Type == "Ready" && c.Status == "True") == true), Timeout);
            await Task.Delay(2000);
            var recorder = Record(informers.Get<V1Pod>().Where(p => p.Metadata.NamespaceProperty == _ns));
            var path = $"/api/v1/namespaces/{_ns}/pods";
            await Eventually(async () => recorder.View.Keys.ToHashSet().SetEquals(await PodKeys()) && requests.OpenWatches == 1, Timeout);
            var eventsBefore = recorder.Events.Count;
            var listsBefore = requests.Lists(path);

            // changes the informer can't see, then history is gone: its resourceVersion is now too old
            proxy.Cut();
            await _cluster.Admin.CoreV1.DeleteNamespacedPodAsync("p0", _ns, gracePeriodSeconds: 0);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "p9"), _ns);
            await Label("p1", "changed");
            // generate enough unrelated churn that the restarted API server's watch cache starts after the informer's version
            for (var i = 0; i < 20; i++)
                await _cluster.Admin.CoreV1.CreateNamespacedConfigMapAsync(new V1ConfigMap { Metadata = new V1ObjectMeta { Name = $"churn-{i}" } }, _ns);
            await KindControlPlane.CompactEtcdAsync();
            await KindControlPlane.RestartApiServerAsync();
            await Eventually(async () =>
            {
                await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns);
                return true;
            }, TimeSpan.FromMinutes(3));
            proxy.Restore();

            await Eventually(async () => recorder.View.Keys.ToHashSet().SetEquals(await PodKeys()) && recorder.View[$"{_ns}/p1"].Metadata.Labels?["tier"] == "changed", TimeSpan.FromMinutes(2),
                async () => $"view [{string.Join(",", recorder.View.Keys)}] cluster [{string.Join(",", await PodKeys())}]");
            requests.Lists(path).Should().BeGreaterThan(listsBefore, "the expired watch forced a relist");
            var after = recorder.Events.Skip(eventsBefore).ToList();
            _output.WriteLine(string.Join("\n", after.Select(e => $"{e.EventFlags} {e.Key}")));
            after.Should().NotContain(e => e.EventFlags.HasFlag(EventTypeFlags.Reset), "a relist never sends a reset");
            after.Should().OnlyContain(e => e.EventFlags.HasFlag(EventTypeFlags.Computed), "missed changes arrive as computed changes");
            after.Select(e => (e.Key, e.EventFlags & ~EventTypeFlags.Computed)).Should().Contain(new[]
            {
                ($"{_ns}/p0", EventTypeFlags.Delete),
                ($"{_ns}/p9", EventTypeFlags.Add),
                ($"{_ns}/p1", EventTypeFlags.Modify),
            });
            after.Should().NotContain(e => e.Key == $"{_ns}/p2" || e.Key == $"{_ns}/p3", "unchanged pods (same resourceVersion) send nothing");
        }

        [Fact]
        public async Task LeaderElection_TwoReplicas_LeaderKilledMidRun_OtherTakesOver()
        {
            SkipIfNoCluster();
            await _cluster.Admin.AppsV1.CreateNamespacedDeploymentAsync(new V1Deployment
            {
                Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = _ns },
                Spec = new V1DeploymentSpec
                {
                    Replicas = 1,
                    Selector = new V1LabelSelector { MatchLabels = new Dictionary<string, string> { ["app"] = "web" } },
                    Template = new V1PodTemplateSpec { Metadata = new V1ObjectMeta { Labels = new Dictionary<string, string> { ["app"] = "web" } }, Spec = PausePod(_ns, null).Spec },
                },
            }, _ns);
            var replicas = new Dictionary<string, Process>
            {
                ["replica-a"] = StartReplica("replica-a"),
                ["replica-b"] = StartReplica("replica-b"),
            };
            try
            {
                async Task<string> Holder() => (await _cluster.Admin.CoordinationV1.ReadNamespacedLeaseAsync("readiness-controller", _ns)).Spec.HolderIdentity;

                async Task<bool> Annotated(int running, string by)
                {
                    var d = await _cluster.Admin.AppsV1.ReadNamespacedDeploymentAsync("web", _ns);
                    return d.GetAnnotation(DeploymentReadinessController.RunningPodsAnnotation) == running.ToString() && d.GetAnnotation(DeploymentReadinessController.ReconciledByAnnotation) == by;
                }

                string leader = null;
                await Eventually(async () => (leader = await Holder()) != null, Timeout);
                await Eventually(() => Annotated(1, leader), Timeout);
                await _cluster.Admin.AppsV1.PatchNamespacedDeploymentScaleAsync(new V1Patch("{\"spec\":{\"replicas\":2}}", V1Patch.PatchType.MergePatch), "web", _ns);
                await Eventually(() => Annotated(2, leader), Timeout, async () => $"leader {leader}");
                (await Holder()).Should().Be(leader, "only the leader reconciles; the follower never wrote");

                replicas[leader].Kill(entireProcessTree: true);
                var follower = replicas.Keys.Single(x => x != leader);
                await Eventually(async () => await Holder() == follower, TimeSpan.FromSeconds(60));
                await _cluster.Admin.AppsV1.PatchNamespacedDeploymentScaleAsync(new V1Patch("{\"spec\":{\"replicas\":3}}", V1Patch.PatchType.MergePatch), "web", _ns);
                await Eventually(() => Annotated(3, follower), Timeout, async () => $"follower {follower}");
                replicas[follower].HasExited.Should().BeFalse();
                _output.WriteLine($"leader {leader} killed, {follower} took over");
            }
            finally
            {
                foreach (var process in replicas.Values)
                {
                    if (!process.HasExited)
                        process.Kill(entireProcessTree: true);
                    _output.WriteLine(process.StandardOutput.ReadToEnd());
                }
            }
        }

        private Process StartReplica(string identity)
        {
            // tests/Integration.Tests/bin/<configuration>/net10.0/
            var configuration = Path.GetFileName(Path.GetDirectoryName(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)));
            var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
            var host = Path.Combine(root, "samples", "informers", "bin", configuration, "net10.0", "informers.dll");
            File.Exists(host).Should().BeTrue($"the sample host is built at {host}");
            var psi = new ProcessStartInfo("dotnet", $"\"{host}\" --Controller=readiness --Namespace={_ns} --LeaderElection:Identity={identity}")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.Environment["KUBECONFIG"] = KubeconfigPath;
            psi.Environment["Logging__LogLevel__Default"] = "Information";
            var process = Process.Start(psi)!;
            process.ErrorDataReceived += (_, e) => { };
            process.BeginErrorReadLine();
            return process;
        }
    }
}
