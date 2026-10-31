using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class TriggerTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        public sealed class Item
        {
            public string Name { get; set; }
            public long Generation { get; set; }
            public Dictionary<string, string> Labels { get; set; } = new Dictionary<string, string>();
            public List<string> Finalizers { get; set; } = new List<string>();
            public string Status { get; set; }
        }

        private static Item I(string name, long generation = 1, string status = null, Dictionary<string, string> labels = null) =>
            new Item { Name = name, Generation = generation, Status = status, Labels = labels ?? new Dictionary<string, string>() };

        private static KeyValuePair<string, Item> P(Item item) => new KeyValuePair<string, Item>(item.Name, item);

        private static ResourceEvent<string, Item> E(EventTypeFlags flags, Item item) => new ResourceEvent<string, Item>(flags, item.Name, item);

        private static async Task<ResourceEvent<TKey, TValue>> Next<TKey, TValue>(IAsyncEnumerator<ResourceEvent<TKey, TValue>> e)
        {
            (await e.MoveNextAsync().TimeoutIfNotDebugging()).Should().BeTrue();
            return e.Current;
        }

        private static RunOptions Fast() => new RunOptions { Backoff = Backoff.Constant(TimeSpan.FromMilliseconds(20)) };

        [Fact]
        public async Task TriggerOn_UnchangedSelection_IsQuietButCarriesTheLatestObject()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a")));
            await using var e = source.TriggerOn(x => x.Generation).GetAsyncEnumerator(Ct);
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);

            var statusOnly = I("a", status: "ready");
            source.Send(E(EventTypeFlags.Modify, statusOnly));
            var quiet = await Next(e);
            quiet.EventFlags.Should().HaveFlag(EventTypeFlags.Modify).And.HaveFlag(EventTypeFlags.Quiet);
            quiet.Value.Should().BeSameAs(statusOnly);

            source.Send(E(EventTypeFlags.Modify, I("a", generation: 2, status: "ready")));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task TriggerOn_AddsDeletesAndSyncsAlwaysTrigger()
        {
            var source = new TestSource<string, Item>();
            source.SendReset();
            await using var e = source.TriggerOn(x => x.Generation).GetAsyncEnumerator(Ct);
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.ResetEmpty);
            source.Send(E(EventTypeFlags.Add, I("a")));
            (await Next(e)).EventFlags.Should().Be(EventTypeFlags.Add);
            source.Send(E(EventTypeFlags.Modify | EventTypeFlags.Sync, I("a")));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);
            source.Send(E(EventTypeFlags.Delete, I("a")));
            (await Next(e)).EventFlags.Should().Be(EventTypeFlags.Delete);
            source.Send(E(EventTypeFlags.Add, I("a")));
            (await Next(e)).EventFlags.Should().Be(EventTypeFlags.Add, "a key deleted and re-added has no previous value");
        }

        [Fact]
        public async Task TriggerOn_ComparesCollectionsStructurally()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a", labels: new Dictionary<string, string> { ["x"] = "1", ["y"] = "2" })));
            await using var e = source.TriggerOn(x => x.Labels, x => x.Finalizers).GetAsyncEnumerator(Ct);
            await Next(e);

            // fresh instances with the same content, keys in another order
            source.Send(E(EventTypeFlags.Modify, I("a", status: "s", labels: new Dictionary<string, string> { ["y"] = "2", ["x"] = "1" })));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);

            source.Send(E(EventTypeFlags.Modify, I("a", labels: new Dictionary<string, string> { ["x"] = "1", ["y"] = "3" })));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);

            var withFinalizer = I("a", labels: new Dictionary<string, string> { ["x"] = "1", ["y"] = "3" });
            withFinalizer.Finalizers.Add("f");
            source.Send(E(EventTypeFlags.Modify, withFinalizer));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task TriggerOn_ComparesAnonymousProjectionsByValue()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a")));
            await using var e = source.TriggerOn(x => new { x.Generation, Deleting = x.Status == "deleting" }).GetAsyncEnumerator(Ct);
            await Next(e);
            source.Send(E(EventTypeFlags.Modify, I("a", status: "other")));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);
            source.Send(E(EventTypeFlags.Modify, I("a", status: "deleting")));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task TriggerWhen_UsesThePreviousObject()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a", generation: 5)));
            await using var e = source.TriggerWhen((before, after) => after.Generation > before.Generation + 1).GetAsyncEnumerator(Ct);
            await Next(e);
            source.Send(E(EventTypeFlags.Modify, I("a", generation: 6)));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);
            source.Send(E(EventTypeFlags.Modify, I("a", generation: 8)));
            (await Next(e)).EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public void Trigger_NamesWhatChanged()
        {
            var trigger = Trigger.On<Item>(x => x.Labels, x => x.Generation);
            var before = trigger.Capture(I("a", labels: new Dictionary<string, string> { ["x"] = "1" }));
            trigger.Changed(before, trigger.Capture(I("a", labels: new Dictionary<string, string> { ["x"] = "1" }))).Should().BeNull();
            trigger.Changed(before, trigger.Capture(I("a", generation: 2, labels: new Dictionary<string, string> { ["x"] = "2" }))).Should().Be("labels, generation");
            Trigger.Any.Changed(null, null).Should().NotBeNull();
        }

        [Fact]
        public async Task RunAsync_QuietChange_DoesNotReconcile_ButTheNextRunSeesIt()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a")));
            var seen = new ConcurrentQueue<string>();
            var requeued = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.TriggerOn(x => x.Generation).RunAsync((e, ct) =>
            {
                seen.Enqueue(e.Value.Status ?? "none");
                if (seen.Count == 2)
                    requeued.TrySetResult(true);
                // first run asks for a re-check later; the quiet change must not cancel it
                return Task.FromResult(seen.Count == 1 ? ReconcileResult.RequeueAfter(TimeSpan.FromMilliseconds(300)) : ReconcileResult.Done);
            }, Fast(), cts.Token);

            await Task.Delay(100, Ct);
            source.Send(E(EventTypeFlags.Modify, I("a", status: "written-by-us")));
            await requeued.Task.TimeoutIfNotDebugging();
            await Task.Delay(100, Ct);
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
            seen.Should().Equal("none", "written-by-us");
        }

        [Fact]
        public async Task RunAsync_QuietThenRealChange_ReconcilesOnce()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a")));
            var seen = new ConcurrentQueue<(long, string)>();
            var gate = new SemaphoreSlim(0);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var run = source.TriggerOn(x => x.Generation).RunAsync(async (e, ct) =>
            {
                seen.Enqueue((e.Value.Generation, e.Value.Status));
                if (seen.Count == 1)
                    await gate.WaitAsync(ct); // hold the key while changes arrive
                return ReconcileResult.Done;
            }, Fast(), cts.Token);

            await Task.Delay(100, Ct);
            source.Send(E(EventTypeFlags.Modify, I("a", status: "quiet")));
            source.Send(E(EventTypeFlags.Modify, I("a", generation: 2, status: "real")));
            await Task.Delay(100, Ct);
            gate.Release();
            await Task.Delay(200, Ct);
            cts.Cancel();
            await run.TimeoutIfNotDebugging();
            seen.Should().Equal((1L, null), (2L, "real"));
        }

        [Fact]
        public async Task Resync_PassesQuietChangesOn_AndSyncsThemLoudly()
        {
            var source = new TestSource<string, Item>();
            source.SendReset(P(I("a")));
            await using var e = source.TriggerOn(x => x.Generation).Resync(TimeSpan.FromMilliseconds(300)).GetAsyncEnumerator(Ct);
            await Next(e);
            source.Send(E(EventTypeFlags.Modify, I("a", status: "s")));
            var quiet = await Next(e);
            quiet.EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);
            var sync = await Next(e);
            sync.EventFlags.Should().HaveFlag(EventTypeFlags.Sync).And.NotHaveFlag(EventTypeFlags.Quiet);
            sync.Value.Status.Should().Be("s");
        }

        [Fact]
        public async Task GroupJoin_ResultChangedOnlyByQuietInput_IsQuiet()
        {
            var owners = new TestSource<string, Item>();
            var children = new TestSource<string, Item>();
            await using var e = owners
                .GroupJoin(children.TriggerOn(x => x.Generation), o => o.Name, c => c.Labels.GetValueOrDefault("owner"), (o, cs) => (o.Name, o.Generation, Statuses: string.Join(",", cs.Select(c => c.Status))))
                .GetAsyncEnumerator(Ct);
            owners.SendReset(P(I("o")));
            children.SendReset(P(I("c", labels: new Dictionary<string, string> { ["owner"] = "o" })));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.ResetStart);

            children.Send(E(EventTypeFlags.Modify, I("c", status: "s1", labels: new Dictionary<string, string> { ["owner"] = "o" })));
            var quiet = await Next(e);
            quiet.EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);
            quiet.Value.Statuses.Should().Be("s1", "the result is recomputed from the latest child");

            owners.Send(E(EventTypeFlags.Modify, I("o", generation: 2)));
            var loud = await Next(e);
            loud.EventFlags.Should().NotHaveFlag(EventTypeFlags.Quiet, "the owner's change triggers");
            loud.Value.Generation.Should().Be(2);

            children.Send(E(EventTypeFlags.Modify, I("c", status: "s2", labels: new Dictionary<string, string> { ["owner"] = "o" })));
            (await Next(e)).EventFlags.Should().HaveFlag(EventTypeFlags.Quiet);
        }
    }
}
