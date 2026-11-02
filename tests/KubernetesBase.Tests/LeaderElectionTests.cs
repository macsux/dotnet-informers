using System;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class LeaderElectionTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private readonly FakeGenericClient _client = new FakeGenericClient { HasStatus = false };
        private DateTime _now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private KubernetesLeaderElection Replica(string identity, double retrySeconds = 2) =>
            new KubernetesLeaderElection(_client, "ops", "my-controller", identity, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(retrySeconds), null, () => _now);

        private Task<V1Lease> Lease() => _client.GetAsync<V1Lease>("my-controller", "ops", Ct);

        [Fact]
        public async Task FirstReplica_CreatesTheLease_OthersWaitWhileItRenews()
        {
            var a = Replica("a");
            var b = Replica("b");
            (await a.TryAcquireOrRenewAsync(Ct)).Should().BeTrue();
            var lease = await Lease();
            lease.Spec.HolderIdentity.Should().Be("a");
            lease.Spec.LeaseDurationSeconds.Should().Be(15);

            (await b.TryAcquireOrRenewAsync(Ct)).Should().BeFalse();
            for (var i = 0; i < 10; i++)
            {
                _now = _now.AddSeconds(2);
                (await a.TryAcquireOrRenewAsync(Ct)).Should().BeTrue("the holder renews");
                (await b.TryAcquireOrRenewAsync(Ct)).Should().BeFalse("the lease keeps changing, so it never looks expired to b");
            }

            (await Lease()).Spec.HolderIdentity.Should().Be("a");
        }

        [Fact]
        public async Task LeaseNotRenewed_IsTakenOver_AndTheOldHolderCantRenew()
        {
            var a = Replica("a");
            var b = Replica("b");
            await a.TryAcquireOrRenewAsync(Ct);
            var heldByA = await Lease();
            (await b.TryAcquireOrRenewAsync(Ct)).Should().BeFalse();

            _now = _now.AddSeconds(14);
            (await b.TryAcquireOrRenewAsync(Ct)).Should().BeFalse("not yet expired on b's clock");
            _now = _now.AddSeconds(2);
            (await b.TryAcquireOrRenewAsync(Ct)).Should().BeTrue("a didn't renew for longer than the lease duration");
            var lease = await Lease();
            lease.Spec.HolderIdentity.Should().Be("b");
            lease.Spec.LeaseTransitions.Should().Be(1);

            (await a.TryAcquireOrRenewAsync(Ct)).Should().BeFalse("b holds it now");
            (await Lease()).Spec.HolderIdentity.Should().Be("b");
        }

        [Fact]
        public async Task Campaign_GrantsAToken_AndDisposeReleasesTheLease()
        {
            using var b = Replica("b", retrySeconds: 0.05);
            var a = Replica("a", retrySeconds: 0.05);
            var token = await a.WaitForLeadershipAsync(Ct).TimeoutIfNotDebugging();
            a.IsLeader.Should().BeTrue();
            var bLeads = b.WaitForLeadershipAsync(Ct);
            await Task.Delay(200, Ct);
            bLeads.IsCompleted.Should().BeFalse();

            a.Dispose();
            token.IsCancellationRequested.Should().BeTrue("disposing ends the term");
            (await Lease()).Spec.HolderIdentity.Should().BeNull("a released the lease");
            await bLeads.TimeoutIfNotDebugging(); // released: taken over without waiting for expiry
            b.IsLeader.Should().BeTrue();
        }
    }
}
