using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;
using static Steeltoe.Informers.Integration.Tests.ClusterFixture;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>The sample controllers against a real cluster, each with an automated assertion of the end state.</summary>
    [Collection(ClusterCollection.Name)]
    public sealed class ControllerScenarioTests : IAsyncLifetime
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(90);
        private readonly ClusterFixture _cluster;
        private readonly ITestOutputHelper _output;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly List<Task> _controllers = new List<Task>();
        private string _ns;
        private k8s.Kubernetes _client;
        private RequestCounter _requests;
        private KubernetesInformerFactory _informers;

        public ControllerScenarioTests(ClusterFixture cluster, ITestOutputHelper output)
        {
            _cluster = cluster;
            _output = output;
        }

        public async ValueTask InitializeAsync()
        {
            if (string.IsNullOrEmpty(KubeconfigPath))
                return;
            _ns = await _cluster.CreateNamespaceAsync(GetType().Name.Substring(0, 4).ToLowerInvariant());
            (_client, _requests) = _cluster.NewClient();
            _informers = new KubernetesInformerFactory(_client, new KubernetesInformerRegistration[] { new KubernetesInformerRegistration<V1Pod>().StripManagedFields() });
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            foreach (var controller in _controllers)
            {
                try
                {
                    await controller.WaitAsync(TimeSpan.FromSeconds(30));
                }
                catch (Exception e)
                {
                    _output.WriteLine($"controller ended with {e}");
                }
            }

            if (_ns != null)
                await _cluster.DeleteNamespaceAsync(_ns);
            _client?.Dispose();
        }

        private void Start(Func<CancellationToken, Task> controller) =>
            _controllers.Add(Task.Run(async () =>
            {
                try
                {
                    await controller(_stop.Token);
                }
                catch (Exception e) when (!_stop.IsCancellationRequested)
                {
                    _output.WriteLine($"controller failed: {e}");
                    throw;
                }
            }));

        private void AssertControllersRunning()
        {
            foreach (var controller in _controllers.Where(x => x.IsFaulted))
                controller.GetAwaiter().GetResult();
        }

        private static V1Deployment Deployment(string ns, string name, int replicas) => new V1Deployment
        {
            Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = ns },
            Spec = new V1DeploymentSpec
            {
                Replicas = replicas,
                Selector = new V1LabelSelector { MatchLabels = new Dictionary<string, string> { ["app"] = name } },
                Template = new V1PodTemplateSpec
                {
                    Metadata = new V1ObjectMeta { Labels = new Dictionary<string, string> { ["app"] = name } },
                    Spec = PausePod(ns, null).Spec,
                },
            },
        };

        private async Task<int> RunningPodsAsync(string labelSelector) =>
            (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns, labelSelector: labelSelector)).Items.Count(p => p.Status?.Phase == "Running" && p.Metadata.DeletionTimestamp == null);

        private Task Scale(string deployment, int replicas) =>
            _cluster.Admin.AppsV1.PatchNamespacedDeploymentScaleAsync(new V1Patch($"{{\"spec\":{{\"replicas\":{replicas}}}}}", V1Patch.PatchType.MergePatch), deployment, _ns);

        [Fact]
        public async Task GroupJoinRunAsyncResync_AnnotatesDeploymentsWithRunningPodCount()
        {
            SkipIfNoCluster();
            await _cluster.Admin.AppsV1.CreateNamespacedDeploymentAsync(Deployment(_ns, "web", 2), _ns);
            await _cluster.Admin.AppsV1.CreateNamespacedDeploymentAsync(Deployment(_ns, "db", 1), _ns);
            var controller = new DeploymentReadinessController(_informers, _client) { ResyncPeriod = TimeSpan.FromSeconds(3), Identity = "test" };
            Start(ct => controller.RunAsync(_ns, null, ct));

            async Task<bool> AnnotationsMatch()
            {
                AssertControllersRunning();
                foreach (var name in new[] { "web", "db" })
                {
                    var deployment = await _cluster.Admin.AppsV1.ReadNamespacedDeploymentAsync(name, _ns);
                    var running = await RunningPodsAsync($"app={name}");
                    if (deployment.GetAnnotation(DeploymentReadinessController.RunningPodsAnnotation) != running.ToString() || running != deployment.Spec.Replicas)
                        return false;
                }

                return true;
            }

            await Eventually(AnnotationsMatch, Timeout, async () => controller.Stats.ToString());
            await Scale("web", 3);
            await Eventually(AnnotationsMatch, Timeout, async () => controller.Stats.ToString());
            await Scale("web", 1);
            await Eventually(AnnotationsMatch, Timeout, async () => controller.Stats.ToString());
            await Eventually(() => controller.Stats["syncs"] >= 2, TimeSpan.FromSeconds(20), () => controller.Stats.ToString());
            _output.WriteLine(controller.Stats.ToString());
        }

        private GenericClient AppSets => new GenericClient(_cluster.Admin, SampleResources.Group, "v1", "appsets", false);

        private async Task<AppSet> CreateAppSet(string name, int replicas) =>
            await AppSets.CreateNamespacedAsync(new AppSet { Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = _ns }, Spec = new AppSetSpec { Replicas = replicas } }, _ns);

        private async Task<List<V1Pod>> PodsOwnedBy(AppSet set) =>
            (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns)).Items.Where(p => p.OwnedBy(set) && p.Metadata.DeletionTimestamp == null).ToList();

        [Fact]
        public async Task CrdReplicaController_Expectations_OwnerUid_Finalizers_Status_Requeue_Conflicts()
        {
            SkipIfNoCluster();
            var controller = new AppSetController(_informers, _client, new KubernetesGenericClient(_client));
            var other = await CreateAppSet("other", 0);
            // a pod that claims to be owned by an AppSet named "web", but by UID belongs to "other": it must never count for "web"
            var decoy = PausePod(_ns, "decoy", new Dictionary<string, string> { [AppSet.OwnerLabel] = "web" });
            decoy.Metadata.OwnerReferences = new List<V1OwnerReference> { new V1OwnerReference { ApiVersion = "samples.steeltoe.io/v1", Kind = "AppSet", Name = "web", Uid = other.Metadata.Uid } };
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(decoy, _ns);
            var web = await CreateAppSet("web", 3);
            Start(ct => controller.RunAsync(_ns, ct));

            async Task<bool> Converged(int replicas)
            {
                AssertControllersRunning();
                var current = await AppSets.ReadNamespacedAsync<AppSet>(_ns, "web");
                var pods = await PodsOwnedBy(current);
                return pods.Count == replicas && pods.All(p => p.Status?.Phase == "Running")
                       && current.Status?.Replicas == replicas && current.Status.ReadyReplicas == replicas && current.Status.ObservedGeneration == current.Metadata.Generation
                       && current.Metadata.Finalizers?.Contains(AppSet.Finalizer) == true
                       && (await PodsOwnedBy(other)).Count == 0;
            }

            await Eventually(() => Converged(3), Timeout, async () => controller.Stats.ToString());
            (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns)).Items.Should().NotContain(p => p.Metadata.Name == "decoy" && p.Metadata.DeletionTimestamp == null, "the decoy belongs to 'other', which wants 0 pods");
            controller.Stats["requeue-after"].Should().BeGreaterThan(0, "the controller re-checks while pods start");

            await AppSets.PatchNamespacedAsync<AppSet>(new V1Patch("{\"spec\":{\"replicas\":5}}", V1Patch.PatchType.MergePatch), _ns, "web");
            await Eventually(() => Converged(5), Timeout, async () => controller.Stats.ToString());
            controller.Stats["creates"].Should().Be(5, "expectations prevent duplicate creates while the watch catches up");

            // no reconcile loop from the controller's own status writes
            var reconciles = controller.Stats["reconciles"];
            var statusWrites = controller.Stats["status-writes"];
            await Task.Delay(TimeSpan.FromSeconds(5));
            controller.Stats["reconciles"].Should().Be(reconciles, "status-only changes don't trigger reconciles");
            controller.Stats["status-writes"].Should().Be(statusWrites);

            // a concurrent change right before the status write: the write gets 409 and RunAsync retries with the latest state
            var raced = 0;
            controller.BeforeStatusWrite = async set =>
            {
                if (Interlocked.Exchange(ref raced, 1) == 0)
                    await AppSets.PatchNamespacedAsync<AppSet>(new V1Patch("{\"metadata\":{\"labels\":{\"touched\":\"yes\"}}}", V1Patch.PatchType.MergePatch), _ns, "web");
            };
            await AppSets.PatchNamespacedAsync<AppSet>(new V1Patch("{\"spec\":{\"replicas\":4}}", V1Patch.PatchType.MergePatch), _ns, "web");
            await Eventually(() => Converged(4), Timeout, async () => controller.Stats.ToString());
            controller.Stats["conflicts"].Should().BeGreaterThanOrEqualTo(1);

            // deletion goes through the finalizer: pods are deleted first, then the AppSet disappears
            web = await AppSets.ReadNamespacedAsync<AppSet>(_ns, "web");
            await AppSets.DeleteNamespacedAsync<AppSet>(_ns, "web");
            await Eventually(async () =>
            {
                AssertControllersRunning();
                try
                {
                    await AppSets.ReadNamespacedAsync<AppSet>(_ns, "web");
                    return false;
                }
                catch (HttpOperationException e) when (e.Response.StatusCode == HttpStatusCode.NotFound)
                {
                    return (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns)).Items.All(p => !p.OwnedBy(web));
                }
            }, Timeout, async () => controller.Stats.ToString());
            controller.Stats["finalized"].Should().BeGreaterThanOrEqualTo(1);
            controller.Stats["requeue"].Should().BeGreaterThanOrEqualTo(1, "the finalizer requeues until the pods are gone");
            _output.WriteLine(controller.Stats.ToString());
        }

        [Fact]
        public async Task MetadataOnly_SecretUsageWithMultiValuedKeys_WithoutLoadingSecretData()
        {
            SkipIfNoCluster();
            foreach (var name in new[] { "s1", "s2", "unused" })
                await _cluster.Admin.CoreV1.CreateNamespacedSecretAsync(new V1Secret { Metadata = new V1ObjectMeta { Name = name }, StringData = new Dictionary<string, string> { ["key"] = "value" } }, _ns);
            var p1 = PausePod(_ns, "p1");
            p1.Spec.Volumes = new List<V1Volume> { new V1Volume { Name = "v", Secret = new V1SecretVolumeSource { SecretName = "s1" } } };
            p1.Spec.Containers[0].Env = new List<V1EnvVar> { new V1EnvVar { Name = "E", ValueFrom = new V1EnvVarSource { SecretKeyRef = new V1SecretKeySelector { Name = "s2", Key = "key" } } } };
            var p2 = PausePod(_ns, "p2");
            p2.Spec.Containers[0].EnvFrom = new List<V1EnvFromSource> { new V1EnvFromSource { SecretRef = new V1SecretEnvSource { Name = "s1" } } };
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(p1, _ns);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(p2, _ns);
            var controller = new SecretUsageController(_informers, _client);
            Start(ct => controller.RunAsync(_ns, ct));

            async Task<bool> UsedBy(string s1, string s2)
            {
                AssertControllersRunning();
                var a = await _cluster.Admin.CoreV1.ReadNamespacedSecretAsync("s1", _ns);
                var b = await _cluster.Admin.CoreV1.ReadNamespacedSecretAsync("s2", _ns);
                var c = await _cluster.Admin.CoreV1.ReadNamespacedSecretAsync("unused", _ns);
                return a.GetAnnotation(SecretUsageController.UsedByAnnotation) == s1 && b.GetAnnotation(SecretUsageController.UsedByAnnotation) == s2
                       && (c.GetAnnotation(SecretUsageController.UsedByAnnotation) ?? "") == "";
            }

            await Eventually(() => UsedBy("p1,p2", "p1"), Timeout, async () => controller.Stats.ToString());
            await _cluster.Admin.CoreV1.DeleteNamespacedPodAsync("p2", _ns, gracePeriodSeconds: 0);
            await Eventually(() => UsedBy("p1", "p1"), Timeout, async () => controller.Stats.ToString());

            var secretRequests = _requests.Requests.Where(x => x.Path.EndsWith("/secrets")).ToList();
            secretRequests.Should().NotBeEmpty().And.OnlyContain(x => x.Accept.Contains("PartialObjectMetadata"), "secret data is never requested");
        }

        [Fact]
        public async Task MetadataOnly_OrphanCleaner_WaitsForSyncBeforeJudging()
        {
            SkipIfNoCluster();
            var replicaSet = new V1ReplicaSet
            {
                Metadata = new V1ObjectMeta { Name = "keep" },
                Spec = new V1ReplicaSetSpec { Replicas = 0, Selector = new V1LabelSelector { MatchLabels = new Dictionary<string, string> { ["rs"] = "keep" } }, Template = new V1PodTemplateSpec { Metadata = new V1ObjectMeta { Labels = new Dictionary<string, string> { ["rs"] = "keep" } }, Spec = PausePod(_ns, null).Spec } },
            };
            await _cluster.Admin.AppsV1.CreateNamespacedReplicaSetAsync(replicaSet, _ns);
            async Task CreatePod(string name, string owner)
            {
                var pod = PausePod(_ns, name, new Dictionary<string, string> { [OrphanPodCleaner.CheckLabel] = "true" });
                pod.Metadata.Annotations = new Dictionary<string, string> { [OrphanPodCleaner.OwnerAnnotation] = owner };
                await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(pod, _ns);
            }

            await CreatePod("owned", "keep");
            await CreatePod("orphan", "missing");
            var controller = new OrphanPodCleaner(_informers, _client);
            Start(ct => controller.RunAsync(_ns, ct));

            async Task<HashSet<string>> LivePods() => (await _cluster.Admin.CoreV1.ListNamespacedPodAsync(_ns, labelSelector: OrphanPodCleaner.CheckLabel)).Items
                .Where(p => p.Metadata.DeletionTimestamp == null).Select(p => p.Metadata.Name).ToHashSet();

            await Eventually(async () => { AssertControllersRunning(); return (await LivePods()).SetEquals(new[] { "owned" }); }, Timeout, async () => controller.Stats.ToString());
            await CreatePod("late-orphan", "missing");
            await Eventually(async () => (await LivePods()).SetEquals(new[] { "owned" }), Timeout, async () => controller.Stats.ToString());
            await Task.Delay(2000);
            (await LivePods()).Should().BeEquivalentTo(new[] { "owned" }, "a pod whose owner exists is never deleted, not even at startup");
            controller.Stats["synced"].Should().Be(1);
            controller.Stats["deleted"].Should().Be(2);
        }

        [Fact]
        public async Task Untyped_WidgetCrdWithoutCSharpType()
        {
            SkipIfNoCluster();
            var objects = _cluster.Admin.CustomObjects;
            await objects.CreateNamespacedCustomObjectAsync(new Dictionary<string, object>
            {
                ["apiVersion"] = "samples.steeltoe.io/v1",
                ["kind"] = "Widget",
                ["metadata"] = new Dictionary<string, object> { ["name"] = "w1" },
                ["spec"] = new Dictionary<string, object> { ["color"] = "red", ["size"] = 3 },
            }, SampleResources.Group, "v1", _ns, "widgets");
            var controller = new WidgetController(_informers, _client);
            Start(ct => controller.RunAsync(_ns, ct));

            async Task<bool> ConfigMapHas(string color, string size)
            {
                AssertControllersRunning();
                try
                {
                    var cm = await _cluster.Admin.CoreV1.ReadNamespacedConfigMapAsync("widget-w1", _ns);
                    return cm.Data?["color"] == color && cm.Data?["size"] == size;
                }
                catch (HttpOperationException e) when (e.Response.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }
            }

            await Eventually(() => ConfigMapHas("red", "3"), Timeout, async () => controller.Stats.ToString());
            await objects.PatchNamespacedCustomObjectAsync(new V1Patch("{\"spec\":{\"color\":\"blue\"}}", V1Patch.PatchType.MergePatch), SampleResources.Group, "v1", _ns, "widgets", "w1");
            await Eventually(() => ConfigMapHas("blue", "3"), Timeout, async () => controller.Stats.ToString());
            await objects.DeleteNamespacedCustomObjectAsync(SampleResources.Group, "v1", _ns, "widgets", "w1");
            await Eventually(async () =>
            {
                try
                {
                    await _cluster.Admin.CoreV1.ReadNamespacedConfigMapAsync("widget-w1", _ns);
                    return false;
                }
                catch (HttpOperationException e) when (e.Response.StatusCode == HttpStatusCode.NotFound)
                {
                    return true;
                }
            }, Timeout, async () => controller.Stats.ToString());
        }

        [Fact]
        public async Task ClusterScopedNodes_JoinedWithPodsByNodeName_FieldSelectorPushedDown()
        {
            SkipIfNoCluster();
            var nodes = (await _cluster.Admin.CoreV1.ListNodeAsync()).Items.Select(n => n.Metadata.Name).OrderBy(x => x).ToList();
            nodes.Count.Should().BeGreaterThanOrEqualTo(2, "the scenario pins pods to two nodes");
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "a1", node: nodes[0]), _ns);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "a2", node: nodes[0]), _ns);
            await _cluster.Admin.CoreV1.CreateNamespacedPodAsync(PausePod(_ns, "b1", node: nodes[1]), _ns);
            var controller = new NodePodCountController(_informers, _client);
            Start(ct => controller.RunAsync(_ns, ct));

            async Task<bool> Counts(int first, int second)
            {
                AssertControllersRunning();
                var all = (await _cluster.Admin.CoreV1.ListNodeAsync()).Items.ToDictionary(n => n.Metadata.Name, n => n.GetAnnotation(NodePodCountController.Annotation(_ns)));
                return all[nodes[0]] == first.ToString() && all[nodes[1]] == second.ToString() && all.Where(x => x.Key != nodes[0] && x.Key != nodes[1]).All(x => x.Value == "0");
            }

            await Eventually(() => Counts(2, 1), Timeout, async () => controller.Stats.ToString());
            await _cluster.Admin.CoreV1.DeleteNamespacedPodAsync("a1", _ns, gracePeriodSeconds: 0);
            await Eventually(() => Counts(1, 1), Timeout, async () => controller.Stats.ToString());

            var podLists = _requests.Requests.Where(x => !x.Watch && x.Path == $"/api/v1/namespaces/{_ns}/pods").ToList();
            podLists.Should().NotBeEmpty("the namespace is pushed down into the URL");
            podLists.Should().OnlyContain(x => x.Query.Contains("fieldSelector=status.phase=Running"));
            _requests.Requests.Should().Contain(x => !x.Watch && x.Path == "/api/v1/nodes");

            try
            {
                foreach (var node in nodes)
                    await _cluster.Admin.CoreV1.PatchNodeAsync(new V1Patch($"{{\"metadata\":{{\"annotations\":{{\"{NodePodCountController.Annotation(_ns)}\":null}}}}}}", V1Patch.PatchType.MergePatch), node);
            }
            catch (Exception)
            {
                // best-effort cleanup
            }
        }
    }
}
