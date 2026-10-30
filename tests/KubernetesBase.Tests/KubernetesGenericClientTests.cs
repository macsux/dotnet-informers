using System;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using Xunit;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesGenericClientTests : IDisposable
    {
        private readonly WireMockServer _server = WireMockServer.Start();
        private readonly IKubernetesResourceClient _client;

        public KubernetesGenericClientTests()
        {
            _client = new KubernetesGenericClient(new k8s.Kubernetes(new KubernetesClientConfiguration { Host = _server.Urls.First() }));
        }

        [Theory]
        [InlineData("", "v1", "pods", "default", "/api/v1/namespaces/default/pods")]
        [InlineData("", "v1", "nodes", null, "/api/v1/nodes")]
        [InlineData("apps", "v1", "deployments", "prod", "/apis/apps/v1/namespaces/prod/deployments")]
        [InlineData("example.com", "v1alpha1", "widgets", null, "/apis/example.com/v1alpha1/widgets")]
        public async Task SendList_BuildsUrlFromGroupVersionResource(string group, string version, string resource, string ns, string path)
        {
            _server.Given(Request.Create().WithPath(path).UsingGet())
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody("{\"items\":[]}"));
            using var response = await _client.SendListAsync(new GroupVersionResource(group, version, resource), ns, new KubernetesListRequest
            {
                LabelSelector = "app=web",
                FieldSelector = "spec.nodeName=n1",
                Limit = 10,
                Accept = KubernetesListRequest.MetadataListAccept,
            }, TestContext.Current.CancellationToken);
            response.IsSuccessStatusCode.Should().BeTrue();
            var request = _server.LogEntries.Single().RequestMessage;
            request.Query["labelSelector"].Single().Should().Be("app=web");
            request.Query["fieldSelector"].Single().Should().Be("spec.nodeName=n1");
            request.Query["limit"].Single().Should().Be("10");
            request.Headers["Accept"].Single().Should().Contain("as=PartialObjectMetadataList");
        }

        [Fact]
        public async Task SendList_UnsuccessfulStatus_ThrowsWithStatusCode()
        {
            _server.Given(Request.Create().UsingGet()).RespondWith(Response.Create().WithStatusCode(410).WithBody("{\"kind\":\"Status\",\"code\":410}"));
            var act = async () => await _client.SendListAsync(new GroupVersionResource("", "v1", "pods"), null, new KubernetesListRequest { Watch = true, ResourceVersion = "1" }, TestContext.Current.CancellationToken);
            (await act.Should().ThrowAsync<Exception>()).Which.IsResourceVersionGone().Should().BeTrue();
        }

        [Fact]
        public async Task IsNamespaced_UsesDiscovery()
        {
            _server.Given(Request.Create().WithPath("/apis/example.com/v1").UsingGet())
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody(
                    "{\"kind\":\"APIResourceList\",\"groupVersion\":\"example.com/v1\",\"resources\":[{\"name\":\"widgets\",\"namespaced\":true,\"kind\":\"Widget\",\"verbs\":[\"list\"],\"singularName\":\"widget\"},{\"name\":\"clusterwidgets\",\"namespaced\":false,\"kind\":\"ClusterWidget\",\"verbs\":[\"list\"],\"singularName\":\"clusterwidget\"}]}"));
            (await _client.IsNamespacedAsync(new GroupVersionResource("example.com", "v1", "widgets"), TestContext.Current.CancellationToken)).Should().BeTrue();
            (await _client.IsNamespacedAsync(new GroupVersionResource("example.com", "v1", "clusterwidgets"), TestContext.Current.CancellationToken)).Should().BeFalse();
        }

        public void Dispose() => _server.Dispose();
    }
}
