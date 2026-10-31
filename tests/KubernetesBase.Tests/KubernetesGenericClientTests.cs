using System;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s;
using k8s.Models;
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
        private readonly IKubernetesGenericClient _generic;

        public KubernetesGenericClientTests()
        {
            var client = new KubernetesGenericClient(new k8s.Kubernetes(new KubernetesClientConfiguration { Host = _server.Urls.First() }));
            _client = client;
            _generic = client;
        }

        private const string CoreDiscovery =
            "{\"kind\":\"APIResourceList\",\"groupVersion\":\"v1\",\"resources\":[{\"name\":\"pods\",\"namespaced\":true,\"kind\":\"Pod\",\"verbs\":[],\"singularName\":\"\"},{\"name\":\"pods/status\",\"namespaced\":true,\"kind\":\"Pod\",\"verbs\":[],\"singularName\":\"\"}]}";

        private const string WidgetDiscovery =
            "{\"kind\":\"APIResourceList\",\"groupVersion\":\"example.com/v1\",\"resources\":[{\"name\":\"widgets\",\"namespaced\":true,\"kind\":\"Widget\",\"verbs\":[],\"singularName\":\"widget\"}]}";

        private void Respond(string path, string method, int status, string body) =>
            _server.Given(Request.Create().WithPath(path).UsingMethod(method))
                .RespondWith(Response.Create().WithStatusCode(status).WithHeader("Content-Type", "application/json").WithBody(body));

        private static V1Pod Pod(string uid = null) => new V1Pod { Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod", Uid = uid } };

        [Fact]
        public async Task Get_Missing_ReturnsNull()
        {
            Respond("/api/v1/namespaces/prod/pods/web", "GET", 404, "{\"kind\":\"Status\",\"reason\":\"NotFound\",\"code\":404}");
            (await _generic.GetAsync<V1Pod>("web", "prod", TestContext.Current.CancellationToken)).Should().BeNull();
        }

        [Fact]
        public async Task List_FollowsContinueTokens()
        {
            _server.Given(Request.Create().WithPath("/api/v1/namespaces/prod/pods").WithParam("continue", "next").UsingGet())
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody("{\"metadata\":{},\"items\":[{\"metadata\":{\"name\":\"b\"}}]}"));
            _server.Given(Request.Create().WithPath("/api/v1/namespaces/prod/pods").UsingGet()).AtPriority(10)
                .RespondWith(Response.Create().WithHeader("Content-Type", "application/json").WithBody("{\"metadata\":{\"continue\":\"next\"},\"items\":[{\"metadata\":{\"name\":\"a\"}}]}"));
            var pods = await _generic.ListAsync<V1Pod>("prod", cancellationToken: TestContext.Current.CancellationToken);
            pods.Select(x => x.Metadata.Name).Should().Equal("a", "b");
        }

        [Fact]
        public async Task Create_SetsTypeMetaAndPostsToTheCollection()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            Respond("/api/v1/namespaces/prod/pods", "POST", 201, "{\"metadata\":{\"name\":\"web\",\"uid\":\"u1\"}}");
            var created = await _generic.CreateAsync(Pod(), new KubernetesWriteOptions { FieldManager = "test" }, TestContext.Current.CancellationToken);
            created.Metadata.Uid.Should().Be("u1");
            var request = _server.LogEntries.Last().RequestMessage;
            request.Body.Should().Contain("\"apiVersion\":\"v1\"").And.Contain("\"kind\":\"Pod\"");
            request.Query["fieldManager"].Single().Should().Be("test");
        }

        [Fact]
        public async Task ReplaceStatus_GoesToTheStatusSubresource()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            Respond("/api/v1/namespaces/prod/pods/web/status", "PUT", 200, "{\"metadata\":{\"name\":\"web\"}}");
            await _generic.ReplaceAsync(Pod(), status: true, cancellationToken: TestContext.Current.CancellationToken);
            _server.LogEntries.Last().RequestMessage.Path.Should().Be("/api/v1/namespaces/prod/pods/web/status");
        }

        [Fact]
        public async Task Patch_SendsThePatchTypesContentType()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            Respond("/api/v1/namespaces/prod/pods/web", "PATCH", 200, "{\"metadata\":{\"name\":\"web\",\"labels\":{\"a\":\"b\"}}}");
            var patched = await _generic.PatchAsync(Pod(), new V1Patch("{\"metadata\":{\"labels\":{\"a\":\"b\"}}}", V1Patch.PatchType.MergePatch), cancellationToken: TestContext.Current.CancellationToken);
            patched.Metadata.Labels["a"].Should().Be("b");
            var request = _server.LogEntries.Last().RequestMessage;
            request.Headers["Content-Type"].Single().Should().StartWith("application/merge-patch+json");
            request.Body.Should().Be("{\"metadata\":{\"labels\":{\"a\":\"b\"}}}");
        }

        [Fact]
        public async Task Apply_SendsTheDesiredStateWithFieldManager()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            Respond("/api/v1/namespaces/prod/pods/web", "PATCH", 200, "{\"metadata\":{\"name\":\"web\"}}");
            await _generic.ApplyAsync(Pod(), "my-controller", force: true, TestContext.Current.CancellationToken);
            var request = _server.LogEntries.Last().RequestMessage;
            request.Headers["Content-Type"].Single().Should().StartWith("application/apply-patch+yaml");
            request.Query["fieldManager"].Single().Should().Be("my-controller");
            request.Query["force"].Single().Should().Be("true");
        }

        [Fact]
        public async Task Delete_SendsUidPrecondition_AndIsFalseWhenGone()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            Respond("/api/v1/namespaces/prod/pods/web", "DELETE", 200, "{\"kind\":\"Status\"}");
            (await _generic.DeleteAsync(Pod("u1"), cancellationToken: TestContext.Current.CancellationToken)).Should().BeTrue();
            _server.LogEntries.Last().RequestMessage.Body.Should().Contain("\"preconditions\":{\"uid\":\"u1\"}");

            _server.Reset();
            Respond("/api/v1/namespaces/prod/pods/web", "DELETE", 404, "{\"kind\":\"Status\",\"reason\":\"NotFound\",\"code\":404}");
            (await _generic.DeleteAsync(Pod("u1"), cancellationToken: TestContext.Current.CancellationToken)).Should().BeFalse();
        }

        [Fact]
        public async Task UntypedObjects_FindTheirResourceThroughDiscovery()
        {
            Respond("/apis/example.com/v1", "GET", 200, WidgetDiscovery);
            Respond("/apis/example.com/v1/namespaces/prod/widgets/w", "PATCH", 200, "{\"apiVersion\":\"example.com/v1\",\"kind\":\"Widget\",\"metadata\":{\"name\":\"w\"}}");
            var widget = UntypedKubernetesObject.FromJson("{\"apiVersion\":\"example.com/v1\",\"kind\":\"Widget\",\"metadata\":{\"name\":\"w\",\"namespace\":\"prod\"}}");
            var info = await _generic.GetResourceInfoAsync(widget, TestContext.Current.CancellationToken);
            info.Resource.Should().Be(new GroupVersionResource("example.com", "v1", "widgets"));
            info.HasStatus.Should().BeFalse();
            var patched = await _generic.PatchAsync(widget, new V1Patch("{}", V1Patch.PatchType.MergePatch), cancellationToken: TestContext.Current.CancellationToken);
            patched.Kind.Should().Be("Widget");
        }

        [Fact]
        public async Task Discovery_ReportsStatusSubresource()
        {
            Respond("/api/v1", "GET", 200, CoreDiscovery);
            var info = await _generic.GetResourceInfoAsync(new GroupVersionResource("", "v1", "pods"), TestContext.Current.CancellationToken);
            info.HasStatus.Should().BeTrue();
            info.Namespaced.Should().BeTrue();
            info.Kind.Should().Be("Pod");
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
