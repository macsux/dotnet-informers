using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesInformerTests
    {
        private static readonly GroupVersionResource Pods = GroupVersionResource.For<V1Pod>();
        private static readonly GroupVersionResource Nodes = GroupVersionResource.For<V1Node>();
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static RetryPolicy FastRetry => new RetryPolicy((e, attempt) => e.IsTransient() && attempt < 10, _ => TimeSpan.FromMilliseconds(10));

        private static V1Pod Pod(string ns, string name, Dictionary<string, string> labels = null, string node = null, string phase = "Running") => new V1Pod
        {
            ApiVersion = "v1",
            Kind = "Pod",
            Metadata = new V1ObjectMeta { NamespaceProperty = ns, Name = name, Labels = labels },
            Spec = new V1PodSpec { NodeName = node, Containers = new List<V1Container> { new V1Container { Name = "c" } } },
            Status = new V1PodStatus { Phase = phase },
        };

        private static KubernetesInformerFactory Factory(FakeApiServer api) => new KubernetesInformerFactory(api, retryPolicy: FastRetry);

        private static async Task<List<ResourceEvent<string, T>>> ReadReset<T>(IAsyncEnumerator<ResourceEvent<string, T>> e)
        {
            var events = new List<ResourceEvent<string, T>>();
            while (await e.MoveNextAsync().TimeoutIfNotDebugging())
            {
                events.Add(e.Current);
                if (e.Current.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                    break;
            }

            return events;
        }

        private static async Task<ResourceEvent<string, T>> Next<T>(IAsyncEnumerator<ResourceEvent<string, T>> e)
        {
            (await e.MoveNextAsync().TimeoutIfNotDebugging()).Should().BeTrue();
            return e.Current;
        }

        private static async Task WaitUntil(Func<bool> condition)
        {
            for (var i = 0; i < 500 && !condition(); i++)
                await Task.Delay(10);
            condition().Should().BeTrue();
        }

        [Fact]
        public async Task ListThenWatch_KeysByNamespaceAndName()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "web"));
            api.Put(Pods, Pod("prod", "web"));
            var informer = Factory(api).Get<V1Pod>();
            await using var e = informer.GetAsyncEnumerator(Ct);
            var reset = await ReadReset(e);
            reset.Select(x => x.Key).Should().BeEquivalentTo("default/web", "prod/web");
            await WaitUntil(() => api.OpenWatches == 1);
            api.Requests.Last().Request.ResourceVersion.Should().Be("102", "the watch starts from the list's resourceVersion");

            api.Put(Pods, Pod("prod", "web", phase: "Failed"));
            var modify = await Next(e);
            modify.EventFlags.Should().HaveFlag(EventTypeFlags.Modify);
            modify.Key.Should().Be("prod/web");
            api.Remove(Pods, "default", "web");
            var delete = await Next(e);
            delete.EventFlags.Should().HaveFlag(EventTypeFlags.Delete);
            delete.Key.Should().Be("default/web");
        }

        [Fact]
        public async Task SeveralConsumers_ShareOneListAndWatch()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            var informer = Factory(api).Get<V1Pod>();
            await using var e1 = informer.GetAsyncEnumerator(Ct);
            await using var e2 = informer.Where(x => x.Status.Phase != null).GetAsyncEnumerator(Ct); // client-side only: same producer
            await using var e3 = informer.ListWatch().GetAsyncEnumerator(Ct);
            await ReadReset(e1);
            await ReadReset(e2);
            await ReadReset(e3);
            await WaitUntil(() => api.OpenWatches == 1);
            api.ListRequests.Should().Be(1);
            api.WatchRequests.Should().Be(1);
        }

        [Fact]
        public async Task EachProducerStartsWithAFreshList_NoResourceVersionSharedAcrossEnumerations()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            var informer = Factory(api).Get<V1Pod>();
            await using (var e = informer.GetAsyncEnumerator(Ct))
                await ReadReset(e);
            await WaitUntil(() => ((KubernetesInformer<V1Pod>)informer).ActiveProducers == 0);
            await using (var e = informer.GetAsyncEnumerator(Ct))
                await ReadReset(e);
            api.Requests.Where(x => !x.Request.Watch).Should().HaveCount(2).And.OnlyContain(x => x.Request.ResourceVersion == null);
        }

        [Fact]
        public async Task PaginatedList_AppliedAsOneReset()
        {
            var api = new FakeApiServer();
            for (var i = 0; i < 7; i++)
                api.Put(Pods, Pod("default", $"p{i}"));
            var informer = (KubernetesInformer<V1Pod>)Factory(api).Get<V1Pod>();
            informer.Producer.PageSize = 3;
            await using var e = informer.GetAsyncEnumerator(Ct);
            var reset = await ReadReset(e);
            reset.Should().HaveCount(7);
            reset.First().EventFlags.Should().HaveFlag(EventTypeFlags.ResetStart);
            api.Requests.Count(x => !x.Request.Watch).Should().Be(3);
            api.Requests.Where(x => !x.Request.Watch).Skip(1).Should().OnlyContain(x => x.Request.Continue != null);
        }

        [Fact]
        public async Task ExpiredContinueToken_RestartsTheList()
        {
            var api = new FakeApiServer();
            for (var i = 0; i < 4; i++)
                api.Put(Pods, Pod("default", $"p{i}"));
            var informer = (KubernetesInformer<V1Pod>)Factory(api).Get<V1Pod>();
            informer.Producer.PageSize = 2;
            var selection = await KubernetesSelection.ResolveAsync(api, Pods, ServerFilter.Empty, Ct);
            // expire the token between the pages
            var first = true;
            var wrapped = new InterceptingApi(api, request =>
            {
                if (request.Continue != null && first)
                {
                    first = false;
                    api.Put(Nodes, new V1Node { Metadata = new V1ObjectMeta { Name = "n" } });
                    api.Compact();
                }
            });
            var producer = new KubernetesProducer<V1Pod>(wrapped, Pods, PayloadShape.Full, KubernetesInformer<V1Pod>.TypedDeserializer, FastRetry, null) { PageSize = 2 };
            var (items, _) = await producer.ListAsync(selection, Ct);
            items.Should().HaveCount(4);
            wrapped.Requests.Count(x => x.Continue == null).Should().Be(2, "the list restarted from the first page");
        }

        [Fact]
        public async Task WatchExpired_Relists_DeliveringComputedChangesOnly()
        {
            foreach (var asWatchError in new[] { true, false })
            {
                var api = new FakeApiServer { CompactionAsWatchError = asWatchError };
                api.Put(Pods, Pod("default", "stays"));
                api.Put(Pods, Pod("default", "changes"));
                api.Put(Pods, Pod("default", "goes"));
                var informer = Factory(api).Get<V1Pod>();
                await using var e = informer.GetAsyncEnumerator(Ct);
                await ReadReset(e);
                await WaitUntil(() => api.OpenWatches == 1);

                // changes the watch never reports: the connection is gone, then history is compacted
                api.DropConnections();
                api.Put(Pods, Pod("default", "changes", phase: "Succeeded"));
                api.Remove(Pods, "default", "goes");
                api.Put(Pods, Pod("default", "new"));
                api.Compact();

                var events = new List<ResourceEvent<string, V1Pod>> { await Next(e), await Next(e), await Next(e) };
                events.Should().OnlyContain(x => x.EventFlags.HasFlag(EventTypeFlags.Computed) && !x.EventFlags.HasFlag(EventTypeFlags.Reset));
                events.Select(x => (x.Key, x.EventFlags & ~EventTypeFlags.Computed)).Should().BeEquivalentTo(new[]
                {
                    ("default/changes", EventTypeFlags.Modify),
                    ("default/new", EventTypeFlags.Add),
                    ("default/goes", EventTypeFlags.Delete),
                });
                // "stays" has the same resourceVersion: nothing
                var extra = e.MoveNextAsync().AsTask();
                (await Task.WhenAny(extra, Task.Delay(200, Ct))).Should().NotBe(extra);
            }
        }

        [Fact]
        public async Task DroppedConnection_RewatchesFromLastVersion_WithoutRelisting()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            var informer = Factory(api).Get<V1Pod>();
            await using var e = informer.GetAsyncEnumerator(Ct);
            await ReadReset(e);
            await WaitUntil(() => api.OpenWatches == 1);
            var rv = api.Put(Pods, Pod("default", "b"));
            await Next(e);
            api.DropConnections();
            await WaitUntil(() => api.OpenWatches == 1);
            api.ListRequests.Should().Be(1);
            api.Requests.Last().Request.ResourceVersion.Should().Be(rv);
            api.Put(Pods, Pod("default", "c"));
            (await Next(e)).Key.Should().Be("default/c");
        }

        [Fact]
        public async Task WatchEnd_RewatchesFromBookmarkVersion()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            var informer = Factory(api).Get<V1Pod>();
            await using var e = informer.GetAsyncEnumerator(Ct);
            await ReadReset(e);
            await WaitUntil(() => api.OpenWatches == 1);
            api.Put(Nodes, new V1Node { Metadata = new V1ObjectMeta { Name = "unrelated" } }); // bumps the global revision
            api.EndWatches();
            await WaitUntil(() => api.OpenWatches == 1);
            api.Requests.Last().Request.ResourceVersion.Should().Be("102");
            api.Requests.Last().Request.AllowWatchBookmarks.Should().BeTrue();
        }

        [Fact]
        public async Task TransientErrors_AreRetried_NonTransient_ReachConsumers()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            api.Failures.Enqueue(new HttpRequestException("connection refused"));
            api.Failures.Enqueue(new HttpRequestException("connection refused"));
            var informer = Factory(api).Get<V1Pod>();
            await using (var e = informer.GetAsyncEnumerator(Ct))
                (await ReadReset(e)).Should().HaveCount(1);

            var forbidden = new FakeApiServer();
            forbidden.Failures.Enqueue(new HttpOperationException("Forbidden") { Response = new HttpResponseMessageWrapper(new HttpResponseMessage(HttpStatusCode.Forbidden), "") });
            await using var failing = Factory(forbidden).Get<V1Pod>().GetAsyncEnumerator(Ct);
            var act = async () => await failing.MoveNextAsync();
            (await act.Should().ThrowAsync<InformerException>()).WithInnerException<HttpOperationException>();
        }

        [Fact]
        public async Task PushedDownFilters_BecomeSelectors_AndShareProducers()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("payments", "web-1", new Dictionary<string, string> { ["app"] = "web" }, node: "n1"));
            api.Put(Pods, Pod("payments", "web-2", new Dictionary<string, string> { ["app"] = "web" }, node: "n2"));
            api.Put(Pods, Pod("payments", "db-1", new Dictionary<string, string> { ["app"] = "db" }, node: "n1"));
            api.Put(Pods, Pod("other", "web-1", new Dictionary<string, string> { ["app"] = "web" }, node: "n1"));
            var informer = Factory(api).Get<V1Pod>();
            var app = "web";
            var a = informer.Where(p => p.Metadata.NamespaceProperty == "payments" && p.GetLabel("app") == app && p.Spec.Containers.Count > 0);
            var b = informer.Where(p => p.GetLabel("app") == "web").Where(p => p.Namespace() == "payments");
            await using var ea = a.GetAsyncEnumerator(Ct);
            await using var eb = b.GetAsyncEnumerator(Ct);
            (await ReadReset(ea)).Select(x => x.Key).Should().BeEquivalentTo("payments/web-1", "payments/web-2");
            (await ReadReset(eb)).Select(x => x.Key).Should().BeEquivalentTo("payments/web-1", "payments/web-2");
            api.ListRequests.Should().Be(1, "both predicates translate to the same server filter");
            var list = api.Requests.Single(x => !x.Request.Watch);
            list.Namespace.Should().Be("payments");
            list.Request.LabelSelector.Should().Be("app=web");

            await using var byNode = informer.Where(p => p.Spec.NodeName == "n1").GetAsyncEnumerator(Ct);
            (await ReadReset(byNode)).Should().HaveCount(3);
            api.Requests.Last(x => !x.Request.Watch).Request.FieldSelector.Should().Be("spec.nodeName=n1");
        }

        [Fact]
        public async Task ServerFilterTransitions_ArriveAsAddsAndDeletes()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a", new Dictionary<string, string> { ["tier"] = "gold" }));
            var informer = Factory(api).Get<V1Pod>();
            await using var e = informer.Where(p => p.GetLabel("tier") == "gold").GetAsyncEnumerator(Ct);
            await ReadReset(e);
            await WaitUntil(() => api.OpenWatches == 1);
            api.Put(Pods, Pod("default", "a", new Dictionary<string, string> { ["tier"] = "silver" }));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Delete);
            api.Put(Pods, Pod("default", "a", new Dictionary<string, string> { ["tier"] = "gold" }));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Add);
        }

        [Fact]
        public async Task ClusterScoped_KeysByName_NamespaceFilterBecomesFieldSelector()
        {
            var api = new FakeApiServer();
            api.ClusterScoped(Nodes);
            api.Put(Nodes, new V1Node { ApiVersion = "v1", Kind = "Node", Metadata = new V1ObjectMeta { Name = "n1" } });
            var informer = Factory(api).Get<V1Node>();
            await using (var e = informer.GetAsyncEnumerator(Ct))
                (await ReadReset(e)).Single().Key.Should().Be("n1");
            await using (var e = informer.Where(n => n.Metadata.NamespaceProperty == "x").GetAsyncEnumerator(Ct))
            {
                (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.ResetEmpty);
                var list = api.Requests.Last(x => !x.Request.Watch);
                list.Namespace.Should().BeNull();
                list.Request.FieldSelector.Should().Be("metadata.namespace=x");
            }
        }

        [Fact]
        public async Task MetadataOnly_RequestsPartialObjectMetadata()
        {
            var api = new FakeApiServer();
            var secrets = GroupVersionResource.For<V1Secret>();
            api.Put(secrets, new V1Secret { ApiVersion = "v1", Kind = "Secret", Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "s", Labels = new Dictionary<string, string> { ["a"] = "b" } }, Data = new Dictionary<string, byte[]> { ["k"] = new byte[] { 1 } } });
            var informer = Factory(api).MetadataOnly<V1Secret>();
            informer.Shape.Should().Be(PayloadShape.MetadataOnly);
            await using var e = informer.GetAsyncEnumerator(Ct);
            var item = (await ReadReset(e)).Single();
            item.Key.Should().Be("default/s");
            item.Value.Kind.Should().Be("PartialObjectMetadata");
            item.Value.Metadata.Labels["a"].Should().Be("b");
            await WaitUntil(() => api.OpenWatches == 1);
            api.Requests.Should().OnlyContain(x => x.Request.Accept.Contains("PartialObjectMetadata"));
        }

        [Fact]
        public async Task Dynamic_GivesTypedMetadataAndRawJson()
        {
            var api = new FakeApiServer();
            var widgets = new GroupVersionResource("example.com", "v1", "widgets");
            api.Put(widgets, new Dictionary<string, object>
            {
                ["apiVersion"] = "example.com/v1",
                ["kind"] = "Widget",
                ["metadata"] = new Dictionary<string, object> { ["namespace"] = "default", ["name"] = "w" },
                ["spec"] = new Dictionary<string, object> { ["size"] = 3 },
            });
            var informer = Factory(api).Dynamic(widgets);
            await using var e = informer.Where(x => x.Metadata.Name == "w").GetAsyncEnumerator(Ct);
            var item = (await ReadReset(e)).Single();
            item.Key.Should().Be("default/w");
            item.Value.Kind.Should().Be("Widget");
            item.Value.Get("spec")!.Value.GetProperty("size").GetInt32().Should().Be(3);
            api.Requests.First().Request.FieldSelector.Should().Be("metadata.name=w");
        }

        [Fact]
        public async Task Transform_RunsBeforeCaching_AndIsPartOfTheRegistration()
        {
            var api = new FakeApiServer();
            var pod = Pod("default", "a");
            pod.Metadata.ManagedFields = new List<V1ManagedFieldsEntry> { new V1ManagedFieldsEntry { Manager = "kubectl" } };
            api.Put(Pods, pod);
            var registration = new KubernetesInformerRegistration<V1Pod>().StripManagedFields();
            var factory = new KubernetesInformerFactory(api, new[] { registration }, retryPolicy: FastRetry);
            await using var e = factory.Get<V1Pod>().GetAsyncEnumerator(Ct);
            (await ReadReset(e)).Single().Value.Metadata.ManagedFields.Should().BeNull();
        }

        [Fact]
        public async Task ExplicitOptions_ShareTheProducerOfAnEquivalentPredicate()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("payments", "a", new Dictionary<string, string> { ["app"] = "web" }));
            var informer = Factory(api).Get<V1Pod>();
            await using var e1 = informer.ListWatch(KubernetesInformerOptions.Builder.NamespaceEquals("payments").LabelEquals("app", "web").Build()).GetAsyncEnumerator(Ct);
            await using var e2 = informer.Where(p => p.Metadata.NamespaceProperty == "payments" && p.GetLabel("app") == "web").GetAsyncEnumerator(Ct);
            await ReadReset(e1);
            await ReadReset(e2);
            api.ListRequests.Should().Be(1);
        }

        [Fact]
        public async Task WaitForSync_AndIsSynced()
        {
            var api = new FakeApiServer();
            api.Put(Pods, Pod("default", "a"));
            var informer = Factory(api).Get<V1Pod>();
            informer.IsSynced.Should().BeFalse();
            var waiting = informer.WaitForSyncAsync(Ct);
            await using var e = informer.GetAsyncEnumerator(Ct);
            var reading = ReadReset(e);
            await waiting.TimeoutIfNotDebugging();
            informer.IsSynced.Should().BeTrue();
            await reading;
        }

        /// <summary>Wraps an API to observe requests before they're served.</summary>
        private sealed class InterceptingApi : IKubernetesResourceClient
        {
            private readonly FakeApiServer _api;
            private readonly Action<KubernetesListRequest> _before;

            public InterceptingApi(FakeApiServer api, Action<KubernetesListRequest> before)
            {
                _api = api;
                _before = before;
            }

            public List<KubernetesListRequest> Requests { get; } = new List<KubernetesListRequest>();

            public Task<HttpResponseMessage> SendListAsync(GroupVersionResource resource, string namespaceParameter, KubernetesListRequest request, CancellationToken cancellationToken = default)
            {
                Requests.Add(request);
                _before(request);
                return _api.SendListAsync(resource, namespaceParameter, request, cancellationToken);
            }

            public Task<bool> IsNamespacedAsync(GroupVersionResource resource, CancellationToken cancellationToken = default) => _api.IsNamespacedAsync(resource, cancellationToken);
        }
    }
}
