using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class OperatorTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static KeyValuePair<string, TestResource> R(string key, string value, int version = 1) =>
            new KeyValuePair<string, TestResource>(key, new TestResource(key, value: value, version: version));

        private static ResourceEvent<string, TestResource> E(EventTypeFlags flags, string key, string value = null, int version = 1) =>
            new ResourceEvent<string, TestResource>(flags, key, new TestResource(key, value: value, version: version));

        private static async Task<ResourceEvent<TKey, TValue>> Next<TKey, TValue>(IAsyncEnumerator<ResourceEvent<TKey, TValue>> e)
        {
            (await e.MoveNextAsync().TimeoutIfNotDebugging()).Should().BeTrue();
            return e.Current;
        }

        private static async Task NothingMore<TKey, TValue>(IAsyncEnumerator<ResourceEvent<TKey, TValue>> e)
        {
            var next = e.MoveNextAsync().AsTask();
            (await Task.WhenAny(next, Task.Delay(100, Ct))).Should().NotBe(next, "no further event was expected");
            // the pending move is abandoned with the enumerator
        }

        [Fact]
        public async Task DistinctUntilChanged_DropsModifiesWithSameSelectedValue_PassesSync()
        {
            var source = new TestSource<string, TestResource>();
            source.SendReset(R("a", "x", version: 1));
            await using var e = source.DistinctUntilChanged(x => x.Version).GetAsyncEnumerator(Ct);
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.ResetStart);

            source.Send(E(EventTypeFlags.Modify, "a", "status-only", version: 1));
            source.Send(E(EventTypeFlags.Modify, "a", "spec", version: 2));
            var modify = await Next(e);
            modify.Value.Value.Should().Be("spec");

            source.Send(E(EventTypeFlags.Modify | EventTypeFlags.Sync, "a", "spec", version: 2));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Sync);
            source.Send(E(EventTypeFlags.Delete, "a", "spec", version: 2));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Delete);
        }

        [Fact]
        public async Task Resync_ReEmitsKnownKeysAsSync_SkipsKeysWithPendingChange()
        {
            var source = new TestSource<string, TestResource>();
            source.SendReset(R("a", "1"), R("b", "1"));
            await using var e = source.Resync(TimeSpan.FromMilliseconds(200)).GetAsyncEnumerator(Ct);
            (await Next(e)).Key.Should().Be("a");
            (await Next(e)).Key.Should().Be("b");

            var synced = new List<ResourceEvent<string, TestResource>> { await Next(e), await Next(e) };
            synced.Should().OnlyContain(x => x.EventFlags.HasFlag(EventTypeFlags.Sync) && x.EventFlags.HasFlag(EventTypeFlags.Modify));
            synced.Select(x => x.Key).Should().BeEquivalentTo("a", "b");
        }

        [Fact]
        public async Task Resync_KeyWithPendingChange_IsNotSyncedAgain()
        {
            var source = new TestSource<string, TestResource>();
            source.SendReset(R("a", "1"), R("b", "1"));
            await using var e = source.Resync(TimeSpan.FromMilliseconds(150)).GetAsyncEnumerator(Ct);
            await Next(e);
            await Next(e);
            source.Send(E(EventTypeFlags.Modify, "a", "2"));
            await Task.Delay(400, Ct); // a change for "a" is pending when the sync is due: it covers it
            var events = new List<ResourceEvent<string, TestResource>> { await Next(e), await Next(e) };
            events.Should().ContainSingle(x => x.Key == "a").Which.EventFlags.Should().NotHaveFlag(EventTypeFlags.Sync);
            events.Should().ContainSingle(x => x.Key == "b").Which.EventFlags.Should().HaveFlag(EventTypeFlags.Sync);
        }

        public sealed record Deployment(string Name, string App);

        public sealed record Pod(string Name, string App, bool Running, string[] Owners = null);

        public sealed record Readiness(string Deployment, int Running);

        [Fact]
        public async Task GroupJoin_StartsWithResetOnceBothSidesSynced_RecomputesOnEitherSide_EmitsOnlyRealChanges()
        {
            var deployments = new TestSource<string, Deployment>();
            var pods = new TestSource<string, Pod>();
            var join = deployments.GroupJoin(pods, d => d.App, p => p.App, (d, ps) => new Readiness(d.Name, ps.Count(p => p.Running)));
            await using var e = join.GetAsyncEnumerator(Ct);

            deployments.SendReset(new KeyValuePair<string, Deployment>("web", new Deployment("web", "web")), new KeyValuePair<string, Deployment>("db", new Deployment("db", "db")));
            var pending = e.MoveNextAsync().AsTask();
            await Task.Delay(50, Ct);
            pending.IsCompleted.Should().BeFalse("the join waits for both sides to sync");
            pods.SendReset(new KeyValuePair<string, Pod>("web-1", new Pod("web-1", "web", true)), new KeyValuePair<string, Pod>("web-2", new Pod("web-2", "web", false)));
            (await pending.TimeoutIfNotDebugging()).Should().BeTrue();
            var reset = new List<ResourceEvent<string, Readiness>> { e.Current, await Next(e) };
            reset.Select(x => x.Value).Should().BeEquivalentTo(new[] { new Readiness("web", 1), new Readiness("db", 0) });
            reset[0].EventFlags.Should().HaveFlag(EventTypeFlags.ResetStart);
            reset[1].EventFlags.Should().HaveFlag(EventTypeFlags.ResetEnd);

            // inner change that doesn't change the result: nothing
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Modify, "web-1", new Pod("web-1", "web", true)));
            // inner change that does
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Modify, "web-2", new Pod("web-2", "web", true)));
            var change = await Next(e);
            change.EventFlags.Should().HaveFlag(EventTypeFlags.Modify);
            change.Value.Should().Be(new Readiness("web", 2));

            // an inner object moving between groups updates both (old and new join key)
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Modify, "web-2", new Pod("web-2", "db", true)));
            var moved = new[] { await Next(e), await Next(e) }.Select(x => x.Value);
            moved.Should().BeEquivalentTo(new[] { new Readiness("web", 1), new Readiness("db", 1) });

            // outer delete
            deployments.Send(new ResourceEvent<string, Deployment>(EventTypeFlags.Delete, "db", new Deployment("db", "db")));
            var deleted = await Next(e);
            deleted.EventFlags.Should().HaveFlag(EventTypeFlags.Delete);
            deleted.Key.Should().Be("db");
        }

        [Fact]
        public async Task GroupJoin_MultiValuedKeys_ManyToMany()
        {
            var owners = Informable.Create(new[] { new Deployment("a", "a"), new Deployment("b", "b") }, x => x.Name);
            var pods = Informable.Create(new[]
            {
                new Pod("p1", null, true, new[] { "a" }),
                new Pod("p2", null, true, new[] { "a", "b" }),
                new Pod("p3", null, true, new[] { "c" }),
            }, x => x.Name);
            var result = await owners.GroupJoin(pods, d => d.Name, p => p.Owners, (d, ps) => (d.Name, Pods: string.Join(",", ps.Select(p => p.Name).OrderBy(x => x)))).ToList();
            result.Should().BeEquivalentTo(new[] { ("a", "p1,p2"), ("b", "p2") });
        }

        [Fact]
        public async Task Join_ResultPerPair_KeyedByPair()
        {
            var owners = new TestSource<string, Deployment>();
            var pods = new TestSource<string, Pod>();
            await using var e = owners.Join(pods, d => d.Name, p => p.Owners, (d, p) => $"{d.Name}/{p.Name}").GetAsyncEnumerator(Ct);
            owners.SendReset(new KeyValuePair<string, Deployment>("a", new Deployment("a", "a")));
            pods.SendReset(new KeyValuePair<string, Pod>("p1", new Pod("p1", null, true, new[] { "a" })));
            var first = await Next(e);
            first.Key.Should().Be(("a", "p1"));
            first.Value.Should().Be("a/p1");

            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Add, "p2", new Pod("p2", null, true, new[] { "a" })));
            (await Next(e)).Key.Should().Be(("a", "p2"));
            // owner reference removed: the pair goes away
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Modify, "p1", new Pod("p1", null, true, Array.Empty<string>())));
            var removed = await Next(e);
            removed.EventFlags.Should().HaveFlag(EventTypeFlags.Delete);
            removed.Key.Should().Be(("a", "p1"));
        }

        public sealed record ReplicaSet(string Name, string Deployment);

        [Fact]
        public async Task JoinOverJoin_Composes_DeploymentReplicaSetPod()
        {
            var deployments = new TestSource<string, Deployment>();
            var replicaSets = new TestSource<string, ReplicaSet>();
            var pods = new TestSource<string, Pod>();
            // replica set -> its pod count, then deployment -> sum over its replica sets
            var rsPods = replicaSets.GroupJoin(pods, rs => rs.Name, p => p.Owners, (rs, ps) => (rs.Deployment, Count: ps.Count()));
            var perDeployment = deployments.GroupJoin(rsPods, d => d.Name, x => x.Deployment, (d, sets) => (d.Name, Pods: sets.Sum(s => s.Count)));
            await using var e = perDeployment.GetAsyncEnumerator(Ct);

            deployments.SendReset(new KeyValuePair<string, Deployment>("web", new Deployment("web", "web")));
            replicaSets.SendReset(new KeyValuePair<string, ReplicaSet>("web-1", new ReplicaSet("web-1", "web")));
            pods.SendReset(new KeyValuePair<string, Pod>("p1", new Pod("p1", "web", true, new[] { "web-1" })));
            (await Next(e)).Value.Should().Be(("web", 1));

            replicaSets.Send(new ResourceEvent<string, ReplicaSet>(EventTypeFlags.Add, "web-2", new ReplicaSet("web-2", "web")));
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Add, "p2", new Pod("p2", "web", true, new[] { "web-2" })));
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Add, "p3", new Pod("p3", "web", true, new[] { "web-2" })));
            var last = await Next(e);
            while (last.Value.Pods != 3)
                last = await Next(e);
            last.Value.Should().Be(("web", 3));
        }

        [Fact]
        public async Task GroupJoin_OverSharedInformers_UsesTheirIndexes_AndAppliesRemainder()
        {
            var deployments = new SharedInformer<string, Deployment>(async (sink, filter, ct) =>
            {
                sink.Replace(new[] { new Deployment("web", "web") });
                await Task.Delay(Timeout.Infinite, ct);
            }, new SharedInformerOptions<string, Deployment> { KeySelector = x => x.Name });
            var podSink = new TaskCompletionSource<IInformerSink<Pod>>();
            var pods = new SharedInformer<string, Pod>(async (sink, filter, ct) =>
            {
                sink.Replace(new[] { new Pod("web-1", "web", true), new Pod("web-2", "web", false) });
                podSink.TrySetResult(sink);
                await Task.Delay(Timeout.Infinite, ct);
            }, new SharedInformerOptions<string, Pod> { KeySelector = x => x.Name });

            var join = deployments.GroupJoin(pods.Where(p => p.Running), d => d.App, p => p.App, (d, ps) => new Readiness(d.Name, ps.Count()));
            await using var e = join.GetAsyncEnumerator(Ct);
            (await Next(e)).Value.Should().Be(new Readiness("web", 1));
            (await podSink.Task).Upsert(new Pod("web-2", "web", true));
            (await Next(e)).Value.Should().Be(new Readiness("web", 2));
            (await podSink.Task).Upsert(new Pod("web-1", "web", false)); // leaves the remainder filter
            (await Next(e)).Value.Should().Be(new Readiness("web", 1));
        }

        [Fact]
        public async Task GroupJoin_ComputedChangesOnEitherSide_FlowThrough()
        {
            var deployments = new TestSource<string, Deployment>();
            var pods = new TestSource<string, Pod>();
            await using var e = deployments.GroupJoin(pods, d => d.App, p => p.App, (d, ps) => new Readiness(d.Name, ps.Count())).GetAsyncEnumerator(Ct);
            deployments.SendReset(new KeyValuePair<string, Deployment>("web", new Deployment("web", "web")));
            pods.SendReset(new KeyValuePair<string, Pod>("web-1", new Pod("web-1", "web", true)));
            await Next(e);
            pods.Send(new ResourceEvent<string, Pod>(EventTypeFlags.Delete | EventTypeFlags.Computed, "web-1", new Pod("web-1", "web", true)));
            var change = await Next(e);
            change.EventFlags.Should().Be(EventTypeFlags.Modify | EventTypeFlags.Computed);
            change.Value.Should().Be(new Readiness("web", 0));
        }

        [Fact]
        public void GroupJoin_OverloadsResolve_ForSingleAndMultiValuedKeys()
        {
            IInformable<string, Deployment> d = new TestSource<string, Deployment>();
            IInformable<string, Pod> p = new TestSource<string, Pod>();
            // compile-time check: single/single, single/multi, multi/multi
            IInformable<string, int> a = d.GroupJoin(p, x => x.App, x => x.App, (x, ps) => ps.Count());
            IInformable<string, int> b = d.GroupJoin(p, x => x.Name, x => x.Owners, (x, ps) => ps.Count());
            IInformable<string, int> c = d.GroupJoin(p, x => new[] { x.Name, x.App }, x => x.Owners, (x, ps) => ps.Count());
            new object[] { a, b, c }.Should().NotContainNulls();
        }
    }
}
