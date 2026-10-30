using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Steeltoe.Informers.Integration.Tests
{
    /// <summary>Disrupts the control plane of a kind cluster: etcd compaction and API server restarts (through docker and crictl).</summary>
    public static class KindControlPlane
    {
        private static string Node => $"{ClusterFixture.KindCluster}-control-plane";

        private const string EtcdCredentials = "--endpoints=https://127.0.0.1:2379 --cacert=/etc/kubernetes/pki/etcd/ca.crt --cert=/etc/kubernetes/pki/etcd/server.crt --key=/etc/kubernetes/pki/etcd/server.key";

        private static async Task<string> ContainerIdAsync(string name) =>
            (await ClusterFixture.RunAsync("docker", $"exec {Node} crictl ps --name {name} -q")).Split('\n', StringSplitOptions.RemoveEmptyEntries).First().Trim();

        /// <summary>Compacts etcd history up to its current revision, so watches from older resource versions get 410 Gone.</summary>
        public static async Task CompactEtcdAsync()
        {
            var etcd = await ContainerIdAsync("etcd");
            var status = await ClusterFixture.RunAsync("docker", $"exec {Node} crictl exec {etcd} etcdctl {EtcdCredentials} endpoint status -w json");
            using var json = JsonDocument.Parse(status);
            var revision = json.RootElement[0].GetProperty("Status").GetProperty("header").GetProperty("revision").GetInt64();
            await ClusterFixture.RunAsync("docker", $"exec {Node} crictl exec {etcd} etcdctl {EtcdCredentials} compact {revision}");
        }

        /// <summary>Stops the API server container; the kubelet starts a fresh one (with an empty watch cache).</summary>
        public static async Task RestartApiServerAsync()
        {
            var apiServer = await ContainerIdAsync("kube-apiserver");
            await ClusterFixture.RunAsync("docker", $"exec {Node} crictl stop {apiServer}");
        }
    }
}
