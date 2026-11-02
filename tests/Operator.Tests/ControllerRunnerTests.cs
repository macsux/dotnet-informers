using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.Operator.Tests
{
    public class ControllerRunnerTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static readonly GroupVersionResource ConfigMaps = GroupVersionResource.For<V1ConfigMap>();
        private static readonly GroupVersionResource Pods = GroupVersionResource.For<V1Pod>();
        private static readonly GroupVersionResource Secrets = GroupVersionResource.For<V1Secret>();
        private static readonly GroupVersionResource Services = GroupVersionResource.For<V1Service>();

        private readonly FakeApiServer _api = new FakeApiServer();
        private readonly TestKubernetesClient _client = new TestKubernetesClient { HasStatus = false };

        private ControllerRunner<V1ConfigMap> Runner(Action<ControllerBuilder<V1ConfigMap>> configure, Func<ReconcileContext<V1ConfigMap>, CancellationToken, Task<ReconcileResult>> reconcile = null)
        {
            var builder = new ControllerBuilder<V1ConfigMap>("test");
            configure(builder);
            var services = new ControllerServices
            {
                Informers = new KubernetesInformerFactory(_api, retryPolicy: new RetryPolicy((_, _) => true, _ => TimeSpan.FromMilliseconds(10))),
                Client = _client,
                Events = NullEventRecorder.Instance,
                DefaultNamespace = "demo",
            };
            return new ControllerRunner<V1ConfigMap>(builder, services, reconcile ?? ((_, _) => Task.FromResult(ReconcileResult.Done)), null);
        }

        private static V1ConfigMap Map(string name, string secret = null, string uid = null) => new V1ConfigMap
        {
            ApiVersion = "v1",
            Kind = "ConfigMap",
            Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = "demo", Uid = uid ?? $"{name}-uid" },
            Data = secret == null ? null : new Dictionary<string, string> { ["secret"] = secret },
        };

        private static V1Pod OwnedPod(string name, string ownerUid, bool controller = true) => new V1Pod
        {
            Metadata = new V1ObjectMeta
            {
                Name = name,
                NamespaceProperty = "demo",
                OwnerReferences = new List<V1OwnerReference> { new V1OwnerReference { ApiVersion = "v1", Kind = "ConfigMap", Name = "x", Uid = ownerUid, Controller = controller } },
            },
        };

        private static async Task<Dictionary<string, ControllerState<V1ConfigMap>>> InitialStateAsync(IInformable<string, ControllerState<V1ConfigMap>> source)
        {
            var result = new Dictionary<string, ControllerState<V1ConfigMap>>();
            await using var e = source.GetAsyncEnumerator(Ct);
            while (await e.MoveNextAsync().TimeoutIfNotDebugging())
            {
                if (!e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                    result[e.Current.Key] = e.Current.Value;
                if (e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                    return result;
            }

            return result;
        }

        [Fact]
        public async Task Relationships_JoinOwnedReferencedAndWatchedObjects()
        {
            _api.Put(ConfigMaps, Map("a", secret: "creds"));
            _api.Put(ConfigMaps, Map("b"));
            _api.Put(ConfigMaps, new V1ConfigMap { Metadata = new V1ObjectMeta { Name = "elsewhere", NamespaceProperty = "other", Uid = "e" } });
            _api.Put(Pods, OwnedPod("a-1", "a-uid"));
            _api.Put(Pods, OwnedPod("a-2", "a-uid", controller: false)); // not the controller reference: not a child
            _api.Put(Pods, OwnedPod("b-1", "b-uid"));
            _api.Put(Secrets, new V1Secret { Metadata = new V1ObjectMeta { Name = "creds", NamespaceProperty = "demo" } });
            _api.Put(Services, new V1Service { Metadata = new V1ObjectMeta { Name = "svc", NamespaceProperty = "demo", Labels = new Dictionary<string, string> { ["configmap"] = "b" } } });

            var runner = Runner(c => c
                .Owns<V1Pod>()
                .References<V1Secret>(m => new[] { m.Data != null && m.Data.TryGetValue("secret", out var s) ? s : null })
                .Watches<V1Service>(s => new[] { $"demo/{s.Metadata.Labels["configmap"]}" }));
            var state = await InitialStateAsync(await runner.BuildAsync(Ct));

            state.Keys.Should().BeEquivalentTo("demo/a", "demo/b");
            var a = runner.CreateContext("demo/a", ReconcileTrigger.Change, state["demo/a"]);
            a.Owned<V1Pod>().Select(x => x.Value.Metadata.Name).Should().Equal("a-1");
            a.Related<V1Secret>().Single().Value.Metadata.Name.Should().Be("creds");
            a.Related<V1Service>().Should().BeEmpty();
            var b = runner.CreateContext("demo/b", ReconcileTrigger.Change, state["demo/b"]);
            b.Owned<V1Pod>().Select(x => x.Value.Metadata.Name).Should().Equal("b-1");
            b.Related<V1Secret>().Should().BeEmpty("b references no secret");
            b.Related<V1Service>().Single().Value.Metadata.Name.Should().Be("svc");
            _api.Requests.Where(x => x.Resource == Pods).Should().OnlyContain(x => x.Namespace == "demo", "related informers are scoped to the controller's namespace");
        }

        [Fact]
        public async Task Finalizers_AreAddedBeforeTheFirstReconcile()
        {
            var map = _client.Store(Map("a"));
            var reconciled = new List<V1ConfigMap>();
            var runner = Runner(c => c.Finalizer("example.com/cleanup", (_, _) => Task.FromResult(ReconcileResult.Done)), (ctx, _) =>
            {
                reconciled.Add(ctx.Object.Original);
                return Task.FromResult(ReconcileResult.Done);
            });
            await runner.HandleAsync(new ResourceEvent<string, ControllerState<V1ConfigMap>>(EventTypeFlags.Add, "demo/a", new ControllerState<V1ConfigMap>(map, 0)), Ct);
            reconciled.Single().Metadata.Finalizers.Should().Contain("example.com/cleanup", "the reconcile sees the object as the finalizer patch left it");
            reconciled.Single().Metadata.ResourceVersion.Should().NotBe(map.Metadata.ResourceVersion);
        }

        [Fact]
        public async Task BeingDeleted_RunsCleanupsInsteadOfReconcile_AndRemovesFinalizersWhenDone()
        {
            var map = Map("a");
            map.Metadata.Finalizers = new List<string> { "example.com/first", "example.com/second" };
            map.Metadata.DeletionTimestamp = DateTime.UtcNow;
            map = _client.Store(map);
            var calls = new List<string>();
            var runner = Runner(c => c
                    .Finalizer("example.com/first", (_, _) =>
                    {
                        calls.Add("first");
                        return Task.FromResult(ReconcileResult.Done);
                    })
                    .Finalizer("example.com/second", (_, _) =>
                    {
                        calls.Add("second");
                        return Task.FromResult(calls.Count(x => x == "second") < 2 ? ReconcileResult.Requeue() : ReconcileResult.Done);
                    }),
                (_, _) =>
                {
                    calls.Add("reconcile");
                    return Task.FromResult(ReconcileResult.Done);
                });

            var first = await runner.HandleAsync(new ResourceEvent<string, ControllerState<V1ConfigMap>>(EventTypeFlags.Modify, "demo/a", new ControllerState<V1ConfigMap>(map, 0)), Ct);
            first.IsRequeue.Should().BeTrue();
            calls.Should().Equal("first", "second");
            var stored = await _client.GetAsync<V1ConfigMap>("a", "demo", Ct);
            stored.Metadata.Finalizers.Should().Equal("example.com/second");

            var second = await runner.HandleAsync(new ResourceEvent<string, ControllerState<V1ConfigMap>>(EventTypeFlags.Modify, "demo/a", new ControllerState<V1ConfigMap>(stored, 0)), Ct);
            second.IsDone.Should().BeTrue();
            calls.Should().Equal("first", "second", "second");
            (await _client.GetAsync<V1ConfigMap>("a", "demo", Ct)).Metadata.Finalizers.Should().BeEmpty();
        }

        [Fact]
        public async Task UnobservedOwnedCreates_HoldTheReconcileBack()
        {
            var map = _client.Store(Map("a"));
            var reconciles = 0;
            var runner = Runner(c => c.Owns<V1Pod>(), async (ctx, ct) =>
            {
                reconciles++;
                await ctx.CreateOwnedAsync(new V1Pod { Metadata = new V1ObjectMeta { GenerateName = "a-" } }, ct);
                return ReconcileResult.Done;
            });
            var e = new ResourceEvent<string, ControllerState<V1ConfigMap>>(EventTypeFlags.Add, "demo/a", new ControllerState<V1ConfigMap>(map, 1));
            await runner.HandleAsync(e, Ct);
            var held = await runner.HandleAsync(e, Ct);
            held.IsRequeueAfter.Should().BeTrue("the created pod isn't observed yet");
            reconciles.Should().Be(1);
        }

        [Fact]
        public async Task ClusterScopedOwner_ExpectationsAreMetWhenNamespacedChildrenAreObserved()
        {
            var namespaces = GroupVersionResource.For<V1Namespace>();
            _api.ClusterScoped(namespaces);
            _client.ClusterScoped.Add(namespaces);
            _api.Put(namespaces, new V1Namespace { Metadata = new V1ObjectMeta { Name = "team", Uid = "team-uid" } });
            var builder = new ControllerBuilder<V1Namespace>("namespaces").AllNamespaces().Owns<V1ConfigMap>();
            var runner = new ControllerRunner<V1Namespace>(builder, new ControllerServices
            {
                Informers = new KubernetesInformerFactory(_api, retryPolicy: new RetryPolicy((_, _) => true, _ => TimeSpan.FromMilliseconds(10))),
                Client = _client,
                Events = NullEventRecorder.Instance,
            }, (_, _) => Task.FromResult(ReconcileResult.Done), null);
            runner.Expectations.ExpectCreations("team", 1); // what CreateOwnedAsync records for the cluster-scoped owner

            await using var e = (await runner.BuildAsync(Ct)).GetAsyncEnumerator(Ct);
            while (await e.MoveNextAsync().TimeoutIfNotDebugging() && !e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
            {
            }

            _api.Put(ConfigMaps, new V1ConfigMap
            {
                Metadata = new V1ObjectMeta
                {
                    Name = "child",
                    NamespaceProperty = "team",
                    OwnerReferences = new List<V1OwnerReference> { new V1OwnerReference { ApiVersion = "v1", Kind = "Namespace", Name = "team", Uid = "team-uid", Controller = true } },
                },
            });
            (await e.MoveNextAsync().TimeoutIfNotDebugging()).Should().BeTrue();
            runner.CreateContext("team", ReconcileTrigger.Change, e.Current.Value).Owned<V1ConfigMap>().Should().ContainSingle();
            runner.Expectations.IsSatisfied("team").Should().BeTrue();
        }

        [Fact]
        public async Task Deleted_ForgetsTheKey()
        {
            string deletedKey = null;
            var builder = new ControllerBuilder<V1ConfigMap>("test").Reconcile((_, _) => Task.FromResult(ReconcileResult.Done)).Deleted((key, _, _) =>
            {
                deletedKey = key;
                return Task.CompletedTask;
            });
            var runner = new ControllerRunner<V1ConfigMap>(builder, new ControllerServices { Client = _client }, builder.ReconcileDelegate, builder.DeletedDelegate);
            (await runner.HandleAsync(new ResourceEvent<string, ControllerState<V1ConfigMap>>(EventTypeFlags.Delete, "demo/a", new ControllerState<V1ConfigMap>(Map("a"), 0)), Ct)).IsDone.Should().BeTrue();
            deletedKey.Should().Be("demo/a");
        }
    }
}
