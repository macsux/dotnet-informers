using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using k8s.Autorest;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.InformersBase.Tests;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using WireMock.Matchers;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesResourceInformerTests : IDisposable
    {
        private readonly ITestOutputHelper _testOutput;
        private WireMockServer _server;
        private KubernetesGenericClient _client;

        public KubernetesResourceInformerTests(ITestOutputHelper testOutput)
        {
            _testOutput = testOutput;
            _server = WireMockServer.Start();
            var kubernetes = new k8s.Kubernetes(new KubernetesClientConfiguration {Host = _server.Urls.First()});
            _client = new KubernetesGenericClient(kubernetes);
            
        }

        [Fact]
        public async Task List()
        {
            _server.Given(Request.Create().WithParam("watch", MatchBehaviour.RejectOnMatch, "true").UsingGet())
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody((KubernetesTestData.ListPodsTwoItems).ToKubernetesJson()));
            var sut = new KubernetesInformer<V1Pod>(_client);

            var result = await sut.List().ToListAsync();

            result.Should().HaveCount(2);
            result[0].Should().BeEquivalentTo(KubernetesTestData.ListPodsTwoItems.Items[0]);
            result[1].Should().BeEquivalentTo(KubernetesTestData.ListPodsTwoItems.Items[1]);
        }

        [Fact]
        public async Task ListWatch()
        {

            _server.Given(Request.Create().UsingGet()).AtPriority(100)
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody((KubernetesTestData.ListPodsTwoItems).ToKubernetesJson()));
            _server.Given(Request.Create().WithParam("watch", MatchBehaviour.AcceptOnMatch, "true").UsingGet())
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody((KubernetesTestData.TestPod1ResourceVersion2.ToWatchEvent(WatchEventType.Modified)).ToKubernetesJson()));

            var sut = new KubernetesInformer<V1Pod>(_client, new RetryPolicy((e, i) => false, i => TimeSpan.Zero), () => false);

            var result = await sut.ListWatch().ToListAsync().TimeoutIfNotDebugging();
            result.Should().HaveCount(3);
            result[0].EventFlags.Should().HaveFlag(EventTypeFlags.ResetStart);
            result[0].Value.Should().BeEquivalentTo(KubernetesTestData.TestPod1ResourceVersion1);
            result[1].EventFlags.Should().HaveFlag(EventTypeFlags.ResetEnd);
            result[1].Value.Should().BeEquivalentTo(KubernetesTestData.TestPod2ResourceVersion1);
            result[2].EventFlags.Should().HaveFlag(EventTypeFlags.Modify);
            result[2].Value.Should().BeEquivalentTo(KubernetesTestData.TestPod1ResourceVersion2);
        }

        [Fact]
        public async Task WatchWithRetryPolicy_WhenApiCallThrowsTransient_ShouldRetry()
        {
            var kubernetes = Substitute.For<IKubernetesGenericClient>();
            kubernetes.ListWithHttpMessagesAsync<V1Pod>().ThrowsForAnyArgs(info => new HttpRequestException());
            var sut = new KubernetesInformer<V1Pod>(kubernetes, new RetryPolicy((e, i) => i < 2, i => TimeSpan.Zero), () => false);
            Func<Task> act = async () => await sut.ListWatch().ToList().TimeoutIfNotDebugging();
            await act.Should().ThrowAsync<HttpRequestException>();
            await kubernetes.ReceivedWithAnyArgs(2).ListWithHttpMessagesAsync<V1Pod>();
            await kubernetes.Received().ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>());
        }
        [Fact]
        public async Task Watch_InterruptedWatchAndGoneResourceVersion_ShouldReList()
        {
            var kubernetes = Substitute.For<IKubernetesGenericClient>();

            kubernetes.ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>())
                .Returns(
                    _ => KubernetesTestData.ListPodEmpty.ToHttpOperationResponse<V1PodList, V1Pod>(),
                    _ => throw new TestCompleteException());
            kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true,
                    resourceVersion: KubernetesTestData.ListPodEmpty.Metadata.ResourceVersion)
                .Returns(KubernetesTestData.TestPod1ResourceVersion1.ToWatchEvent(WatchEventType.Added).ToHttpOperationResponse());
            kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true,
                    resourceVersion: KubernetesTestData.TestPod1ResourceVersion1.Metadata.ResourceVersion)
                .Returns(new HttpOperationResponse<KubernetesList<V1Pod>>() { Response = new HttpResponseMessage() { StatusCode = HttpStatusCode.Gone } });

            var sut = new KubernetesInformer<V1Pod>(kubernetes, RetryPolicy.None, () => true);
            await sut.ListWatch().ToList().TimeoutIfNotDebugging();
           
            await Task.Delay(1000);
            Received.InOrder(() =>
            {
                // initial list
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>());

                // watch after list
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true,
                    resourceVersion: KubernetesTestData.ListPodEmpty.Metadata.ResourceVersion);

                // resume watch with same resource version - server responded with gone
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true,
                    resourceVersion: KubernetesTestData.TestPod1ResourceVersion1.Metadata.ResourceVersion);
                // restart the whole thing with list without version
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>());
            });
        }
        [Fact]
        public async Task Watch_BookmarkInterrupted_ShouldRewatchWithBookmarkResourceVersion()
        {
            var kubernetes = Substitute.For<IKubernetesGenericClient>();

            kubernetes.ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>())
                .Returns(_ => KubernetesTestData.ListPodEmpty.ToHttpOperationResponse<V1PodList, V1Pod>());
            kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    watch: true,
                    resourceVersion: KubernetesTestData.ListPodEmpty.Metadata.ResourceVersion,
                    allowWatchBookmarks: true,
                    cancellationToken: Arg.Any<CancellationToken>())
                .Returns(
                _ => new V1Pod()
                {
                    Kind = V1Pod.KubeKind,
                    ApiVersion = V1Pod.KubeApiVersion,
                    Metadata = new V1ObjectMeta()
                    {
                        ResourceVersion = KubernetesTestData.ListPodOneItem.Metadata.ResourceVersion
                    }
                }
                    .ToWatchEvent(WatchEventType.Bookmark)
                    .ToHttpOperationResponse());
            kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    watch: true,
                    allowWatchBookmarks: true,
                    resourceVersion: KubernetesTestData.ListPodOneItem.Metadata.ResourceVersion,
                    cancellationToken: Arg.Any<CancellationToken>())
                .Throws<TestCompleteException>();

            var sut = new KubernetesInformer<V1Pod>(kubernetes, RetryPolicy.None, () => true);
            var list = await sut.ListWatch().ToList();
            await Task.Delay(1000);
            // Func<Task> act = async () => 
            // list.Take(10).ToList();
            // act.Should().Throw<TestCompleteException>();
            Received.InOrder(() =>
            {
                // initial list
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(cancellationToken: Arg.Any<CancellationToken>());
                // watch after list with same version as returned by list - receive bookmark with new version
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    resourceVersion: KubernetesTestData.ListPodEmpty.Metadata.ResourceVersion,
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true);
                // resume watch with bookmark version
                kubernetes.ListWithHttpMessagesAsync<V1Pod>(
                    resourceVersion: KubernetesTestData.ListPodOneItem.Metadata.ResourceVersion,
                    cancellationToken: Arg.Any<CancellationToken>(),
                    watch: true,
                    allowWatchBookmarks: true);
            });
        }

        public void Dispose()
        {
            _server?.Dispose();
        }
    }
}
