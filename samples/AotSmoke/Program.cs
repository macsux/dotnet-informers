using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Operator;

namespace Steeltoe.Informers.Samples.AotSmoke
{
    /// <summary>
    ///     Exercises the library in a NativeAOT app against a real cluster: typed, metadata-only and untyped informers, a reconcile loop, writes
    ///     through the generic client, and a controller class hosted by the operator. Exits non-zero if a check fails.
    /// </summary>
    /// <remarks>
    ///     Usage: <c>AotSmoke http://127.0.0.1:8001</c> against <c>kubectl proxy</c>, or <c>AotSmoke</c> in a pod (in-cluster config). KubernetesClient
    ///     21 parses kubeconfig files with reflection-based YAML, which fails under NativeAOT, so the smoke test doesn't read one.
    /// </remarks>
    public static class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var config = args.Length > 0 ? new KubernetesClientConfiguration { Host = args[0] } : KubernetesClientConfiguration.InClusterConfig();
            using var kubernetes = new k8s.Kubernetes(config);
            var client = new KubernetesGenericClient(kubernetes);
            var informers = new KubernetesInformerFactory(kubernetes);
            var ns = $"aot-smoke-{Guid.NewGuid():N}"[..20];
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            var ct = timeout.Token;
            var checks = new Checks();
            await Webhooks.CheckAsync(checks, ct);
            await client.CreateAsync(new V1Namespace { Metadata = new V1ObjectMeta { Name = ns } }, cancellationToken: ct);
            try
            {
                using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var seen = new ConcurrentDictionary<string, string>();
                var reconciled = new ConcurrentDictionary<string, string>();
                var metadataOnly = new ConcurrentDictionary<string, string>();
                var untyped = new ConcurrentDictionary<string, string>();
                var loop = informers.Get<V1ConfigMap>().Where(x => x.Metadata.NamespaceProperty == ns)
                    .RunAsync((e, _) =>
                    {
                        if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                            reconciled.TryRemove(e.Key, out var _);
                        else
                            reconciled[e.Key] = e.Value.Data != null && e.Value.Data.TryGetValue("v", out var v) ? v : null;
                        return Task.FromResult(ReconcileResult.Done);
                    }, new RunOptions { Name = "smoke" }, stop.Token);
                var metadataTask = Track(informers.MetadataOnly<V1ConfigMap>().Where(x => x.Metadata.NamespaceProperty == ns), x => x.Metadata.ResourceVersion, metadataOnly, stop.Token);
                var untypedTask = Track(informers.Dynamic(new GroupVersionResource("", "v1", "configmaps")).Where(x => x.Metadata.NamespaceProperty == ns),
                    x => x.Get("data") is { } data && data.TryGetProperty("v", out var v) ? v.GetString() : null, untyped, stop.Token);

                var created = await client.CreateAsync(new V1ConfigMap { Metadata = new V1ObjectMeta { Name = "cm", NamespaceProperty = ns }, Data = new Dictionary<string, string> { ["v"] = "1" } }, cancellationToken: ct);
                checks.Check("create returns the stored object", created.Metadata.Uid != null);
                await checks.Eventually("reconcile sees the add", () => reconciled.GetValueOrDefault($"{ns}/cm") == "1", ct);
                await checks.Eventually("untyped informer sees the add", () => untyped.GetValueOrDefault($"{ns}/cm") == "1", ct);
                await checks.Eventually("metadata-only informer sees the add", () => metadataOnly.ContainsKey($"{ns}/cm"), ct);

                var patched = await client.PatchAsync(created, new V1Patch("{\"data\":{\"v\":\"2\"}}", V1Patch.PatchType.MergePatch), cancellationToken: ct);
                checks.Check("patch returns the patched object", patched.Data["v"] == "2");
                await checks.Eventually("reconcile sees the patch", () => reconciled.GetValueOrDefault($"{ns}/cm") == "2", ct);

                var clone = patched.DeepClone();
                clone.Data["v"] = "3";
                var replaced = await client.ReplaceAsync(clone, cancellationToken: ct);
                checks.Check("replace with the current resourceVersion succeeds", replaced.Data["v"] == "3");
                try
                {
                    await client.ReplaceAsync(clone, cancellationToken: ct);
                    checks.Check("replace with a stale resourceVersion conflicts", false);
                }
                catch (Exception ex) when (ex.IsConflict())
                {
                    checks.Check("replace with a stale resourceVersion conflicts", true);
                }

                var fetched = await client.GetAsync<V1ConfigMap>("cm", ns, ct);
                checks.Check("get returns the object", fetched?.Data["v"] == "3");
                var listed = await client.ListAsync<V1ConfigMap>(ns, cancellationToken: ct);
                checks.Check("list returns the object", listed.Any(x => x.Metadata.Name == "cm"));

                checks.Check("delete returns true", await client.DeleteAsync(fetched, cancellationToken: ct));
                await checks.Eventually("reconcile sees the delete", () => !reconciled.ContainsKey($"{ns}/cm"), ct);
                checks.Check("get after delete returns null", await client.GetAsync<V1ConfigMap>("cm", ns, ct) == null);

                stop.Cancel();
                await Task.WhenAll(loop, metadataTask, untypedTask);

                await OperatorChecksAsync(kubernetes, client, ns, checks, ct);
            }
            finally
            {
                await client.DeleteAsync(new V1Namespace { Metadata = new V1ObjectMeta { Name = ns } }, cancellationToken: CancellationToken.None);
            }

            Console.WriteLine(checks.Failed == 0 ? $"All {checks.Passed} checks passed" : $"{checks.Failed} of {checks.Passed + checks.Failed} checks failed");
            return checks.Failed == 0 ? 0 : 1;
        }

        /// <summary>A controller class hosted by the operator: owned child, finalizer, save, events.</summary>
        private static async Task OperatorChecksAsync(k8s.Kubernetes kubernetes, KubernetesGenericClient client, string ns, Checks checks, CancellationToken ct)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(kubernetes);
            services.AddSingleton<IKubernetes>(kubernetes);
            services.AddSingleton(client);
            services.AddSingleton<IKubernetesGenericClient>(client);
            services.AddKubernetesOperator(o => o.Namespace(ns).LeaderElection("smoke-operator", ns, "smoke").Events(e => e.ReportingController = "smoke.steeltoe.io/mirror"))
                .AddController<MirrorController>();
            await using var provider = services.BuildServiceProvider();
            var hosts = provider.GetServices<IHostedService>().ToList();
            foreach (var host in hosts)
                await host.StartAsync(ct);
            try
            {
                await client.CreateAsync(new V1ConfigMap { Metadata = new V1ObjectMeta { Name = "cm", NamespaceProperty = ns }, Data = new Dictionary<string, string> { ["k"] = "v" } }, cancellationToken: ct);
                await checks.EventuallyAsync("controller creates the owned secret", async () =>
                    (await client.GetAsync<V1Secret>("cm-mirror", ns, ct))?.Metadata.OwnerReferences?.Any(x => x.Kind == "ConfigMap" && x.Controller == true) == true, ct);
                await checks.EventuallyAsync("controller adds its finalizer and saves its label", async () =>
                {
                    var map = await client.GetAsync<V1ConfigMap>("cm", ns, ct);
                    return map.HasFinalizer(MirrorController.Finalizer) && map.Metadata.Labels?.ContainsKey(MirrorController.MirroredLabel) == true;
                }, ct);
                checks.Check("the operator holds its lease", (await client.GetAsync<V1Lease>("smoke-operator", ns, ct))?.Spec?.HolderIdentity == "smoke");
                await checks.EventuallyAsync("controller records an event", async () =>
                    (await client.ListAsync<Eventsv1Event>(ns, cancellationToken: ct)).Any(x => x.Reason == "Mirrored" && x.ReportingController == "smoke.steeltoe.io/mirror"), ct);

                var current = await client.GetAsync<V1ConfigMap>("cm", ns, ct);
                await client.DeleteAsync(current, cancellationToken: ct);
                await checks.EventuallyAsync("finalizer deletes the secret, then the config map goes", async () =>
                    await client.GetAsync<V1ConfigMap>("cm", ns, ct) == null && await client.GetAsync<V1Secret>("cm-mirror", ns, ct) == null, ct);
            }
            finally
            {
                foreach (var host in hosts)
                    await host.StopAsync(CancellationToken.None);
            }
        }

        private static async Task Track<T>(IInformable<string, T> source, Func<T, string> value, ConcurrentDictionary<string, string> state, CancellationToken ct)
        {
            try
            {
                await foreach (var e in source.WithCancellation(ct))
                {
                    if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
                        state.TryRemove(e.Key, out _);
                    else if (!e.EventFlags.HasFlag(EventTypeFlags.ResetEmpty))
                        state[e.Key] = value(e.Value);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        internal sealed class Checks
        {
            public int Passed { get; private set; }
            public int Failed { get; private set; }

            public void Check(string name, bool ok)
            {
                Console.WriteLine($"{(ok ? "PASS" : "FAIL")} {name}");
                if (ok)
                    Passed++;
                else
                    Failed++;
            }

            public async Task EventuallyAsync(string name, Func<Task<bool>> condition, CancellationToken ct)
            {
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (!await condition() && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
                    await Task.Delay(200, CancellationToken.None);
                Check(name, await condition());
            }

            public async Task Eventually(string name, Func<bool> condition, CancellationToken ct)
            {
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (!condition() && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
                    await Task.Delay(100, CancellationToken.None);
                Check(name, condition());
            }
        }
    }
}
