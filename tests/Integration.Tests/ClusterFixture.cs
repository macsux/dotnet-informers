using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>
    ///     A real cluster for the scenario tests. Set <c>INFORMERS_INTEGRATION_KUBECONFIG</c> to a kubeconfig (e.g. a kind cluster) to run them;
    ///     without it every scenario is skipped. Disruption scenarios that restart the API server or compact etcd also need
    ///     <c>INFORMERS_INTEGRATION_KIND_CLUSTER</c> (the kind cluster name) and docker.
    /// </summary>
    public sealed class ClusterFixture : IAsyncLifetime
    {
        public static readonly string KubeconfigPath = Environment.GetEnvironmentVariable("INFORMERS_INTEGRATION_KUBECONFIG");
        public static readonly string KindCluster = Environment.GetEnvironmentVariable("INFORMERS_INTEGRATION_KIND_CLUSTER");

        public KubernetesClientConfiguration Config { get; private set; }

        /// <summary>A client for arranging and asserting (not used by the controllers under test).</summary>
        public k8s.Kubernetes Admin { get; private set; }

        public static void SkipIfNoCluster() => Assert.SkipWhen(string.IsNullOrEmpty(KubeconfigPath), "INFORMERS_INTEGRATION_KUBECONFIG is not set");

        public async ValueTask InitializeAsync()
        {
            if (string.IsNullOrEmpty(KubeconfigPath))
                return;
            Config = KubernetesClientConfiguration.BuildConfigFromConfigFile(KubeconfigPath);
            Admin = new k8s.Kubernetes(Config);
            foreach (var crd in new[] { "appsets.yaml", "widgets.yaml" })
                await ApplyCrdAsync(KubernetesYaml.Deserialize<V1CustomResourceDefinition>(SampleResources.Crd(crd)));
        }

        public ValueTask DisposeAsync()
        {
            Admin?.Dispose();
            return default;
        }

        /// <summary>A client whose requests are counted, for the informers under test.</summary>
        public (k8s.Kubernetes Client, RequestCounter Counter) NewClient(Uri server = null)
        {
            var config = KubernetesClientConfiguration.BuildConfigFromConfigFile(KubeconfigPath);
            if (server != null)
                config.Host = server.ToString();
            var counter = new RequestCounter();
            return (new k8s.Kubernetes(config, counter), counter);
        }

        public async Task<string> CreateNamespaceAsync(string prefix)
        {
            var name = $"it-{prefix}-{Guid.NewGuid().ToString("N").Substring(0, 6)}";
            await Admin.CoreV1.CreateNamespaceAsync(new V1Namespace { Metadata = new V1ObjectMeta { Name = name } });
            // the default service account must exist before pods can be created
            await Eventually(async () => (await Admin.CoreV1.ListNamespacedServiceAccountAsync(name)).Items.Any(x => x.Metadata.Name == "default"), TimeSpan.FromSeconds(30));
            return name;
        }

        public async Task DeleteNamespaceAsync(string name)
        {
            try
            {
                await Admin.CoreV1.DeleteNamespaceAsync(name);
            }
            catch (HttpOperationException e) when (e.Response.StatusCode == HttpStatusCode.NotFound)
            {
            }
        }

        private async Task ApplyCrdAsync(V1CustomResourceDefinition crd)
        {
            try
            {
                await Admin.ApiextensionsV1.CreateCustomResourceDefinitionAsync(crd);
            }
            catch (HttpOperationException e) when (e.Response.StatusCode == HttpStatusCode.Conflict)
            {
            }

            await Eventually(async () =>
            {
                var current = await Admin.ApiextensionsV1.ReadCustomResourceDefinitionAsync(crd.Metadata.Name);
                return current.Status?.Conditions?.Any(x => x.Type == "Established" && x.Status == "True") == true;
            }, TimeSpan.FromSeconds(60));
        }

        /// <summary>Polls until the condition holds; fails with the last observed description otherwise.</summary>
        public static async Task Eventually(Func<Task<bool>> condition, TimeSpan timeout, Func<Task<string>> describe = null)
        {
            var sw = Stopwatch.StartNew();
            Exception last = null;
            while (sw.Elapsed < timeout)
            {
                try
                {
                    if (await condition())
                        return;
                    last = null;
                }
                catch (Exception e)
                {
                    last = e;
                }

                await Task.Delay(250);
            }

            var description = describe != null ? await describe() : null;
            Assert.Fail($"Condition not met within {timeout}. {description} {last}");
        }

        public static Task Eventually(Func<bool> condition, TimeSpan timeout, Func<string> describe = null) =>
            Eventually(() => Task.FromResult(condition()), timeout, describe == null ? null : () => Task.FromResult(describe()));

        public static V1Pod PausePod(string ns, string name, Dictionary<string, string> labels = null, string node = null) => new V1Pod
        {
            Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = ns, Labels = labels },
            Spec = new V1PodSpec
            {
                NodeName = node,
                TerminationGracePeriodSeconds = 0,
                Tolerations = new List<V1Toleration> { new V1Toleration { OperatorProperty = "Exists" } },
                Containers = new List<V1Container> { new V1Container { Name = "pause", Image = "registry.k8s.io/pause:3.10" } },
            },
        };

        /// <summary>Runs a command and returns its output (throws on a non-zero exit).</summary>
        public static async Task<string> RunAsync(string file, string arguments, TimeSpan? timeout = null)
        {
            var psi = new ProcessStartInfo(file, arguments) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi)!;
            var output = process.StandardOutput.ReadToEndAsync();
            var error = process.StandardError.ReadToEndAsync();
            using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromMinutes(2));
            await process.WaitForExitAsync(cts.Token);
            if (process.ExitCode != 0)
                throw new InvalidOperationException($"{file} {arguments} exited with {process.ExitCode}: {await error}");
            return await output;
        }
    }

    /// <summary>Counts the requests a client sends: list and watch requests per path.</summary>
    public sealed class RequestCounter : DelegatingHandler
    {
        public ConcurrentQueue<(string Path, string Query, bool Watch, string Accept)> Requests { get; } = new ConcurrentQueue<(string, string, bool, string)>();

        private int _openWatches;
        public int OpenWatches => Volatile.Read(ref _openWatches);

        public int Lists(string pathSuffix) => Requests.Count(x => !x.Watch && x.Path.EndsWith(pathSuffix, StringComparison.Ordinal));
        public int Watches(string pathSuffix) => Requests.Count(x => x.Watch && x.Path.EndsWith(pathSuffix, StringComparison.Ordinal));

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var query = request.RequestUri!.Query;
            var watch = query.Contains("watch=true", StringComparison.Ordinal);
            if (request.Method == HttpMethod.Get)
                Requests.Enqueue((request.RequestUri.AbsolutePath, Uri.UnescapeDataString(query), watch, request.Headers.Accept.ToString()));
            var response = await base.SendAsync(request, cancellationToken);
            if (watch && response.IsSuccessStatusCode)
            {
                Interlocked.Increment(ref _openWatches);
                response.Content = new TrackedContent(response.Content, () => Interlocked.Decrement(ref _openWatches));
            }

            return response;
        }

        /// <summary>Calls back when the response body (a watch stream) is disposed.</summary>
        private sealed class TrackedContent : HttpContent
        {
            private readonly HttpContent _inner;
            private Action _onDispose;

            public TrackedContent(HttpContent inner, Action onDispose)
            {
                _inner = inner;
                _onDispose = onDispose;
                foreach (var header in inner.Headers)
                    Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context) => _inner.CopyToAsync(stream, context);
            protected override Task<Stream> CreateContentReadStreamAsync() => _inner.ReadAsStreamAsync();
            protected override bool TryComputeLength(out long length)
            {
                length = 0;
                return false;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    Interlocked.Exchange(ref _onDispose, null)?.Invoke();
                    _inner.Dispose();
                }

                base.Dispose(disposing);
            }
        }
    }

    [CollectionDefinition(Name)]
    public sealed class ClusterCollection : ICollectionFixture<ClusterFixture>
    {
        public const string Name = "cluster";
    }
}
