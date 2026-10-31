using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesTriggerTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static V1Pod Pod(string image = "nginx", string phase = null, Dictionary<string, string> labels = null, long generation = 1, string resourceVersion = "1", DateTime? deleting = null) => new V1Pod
        {
            Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod", Generation = generation, Labels = labels, ResourceVersion = resourceVersion, DeletionTimestamp = deleting },
            Spec = new V1PodSpec { Containers = new List<V1Container> { new V1Container { Name = "app", Image = image } } },
            Status = phase == null ? null : new V1PodStatus { Phase = phase },
        };

        private static ResourceEvent<string, V1Pod> Modify(V1Pod pod) => new ResourceEvent<string, V1Pod>(EventTypeFlags.Modify, pod.ObjectKey(), pod);

        private static async Task<EventTypeFlags> NextFlags(IAsyncEnumerator<ResourceEvent<string, V1Pod>> e)
        {
            (await e.MoveNextAsync().TimeoutIfNotDebugging()).Should().BeTrue();
            return e.Current.EventFlags;
        }

        private static TestSource<string, V1Pod> Source(V1Pod initial)
        {
            var source = new TestSource<string, V1Pod>();
            source.SendReset(new KeyValuePair<string, V1Pod>(initial.ObjectKey(), initial));
            return source;
        }

        [Fact]
        public async Task SelectedObjects_CompareByContent_NotReference()
        {
            var source = Source(Pod());
            await using var e = source.TriggerOn(x => x.Spec).GetAsyncEnumerator(Ct);
            await NextFlags(e);
            source.Send(Modify(Pod(phase: "Running")));
            (await NextFlags(e)).Should().HaveFlag(EventTypeFlags.Quiet, "a fresh but equal spec");
            source.Send(Modify(Pod(image: "nginx:2", phase: "Running")));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task DeletionStarting_AlwaysTriggers()
        {
            var source = Source(Pod());
            await using var e = source.TriggerOn(x => x.Spec).GetAsyncEnumerator(Ct);
            await NextFlags(e);
            source.Send(Modify(Pod(deleting: DateTime.UtcNow)));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet, "finalizers must run whatever the selection");
        }

        [Fact]
        public async Task AllButStatus_IgnoresStatus_ReactsToMetadata()
        {
            var source = Source(Pod());
            await using var e = source.TriggerOn(Trigger.AllButStatus).GetAsyncEnumerator(Ct);
            await NextFlags(e);
            source.Send(Modify(Pod(phase: "Running")));
            (await NextFlags(e)).Should().HaveFlag(EventTypeFlags.Quiet);
            source.Send(Modify(Pod(phase: "Running", labels: new Dictionary<string, string> { ["tier"] = "web" })));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task Generation_PresetCombinesWithSelectedValues()
        {
            var source = Source(Pod());
            await using var e = source.TriggerOn(Trigger.Generation, x => x.Metadata.Labels).GetAsyncEnumerator(Ct);
            await NextFlags(e);
            source.Send(Modify(Pod(image: "other")));
            (await NextFlags(e)).Should().HaveFlag(EventTypeFlags.Quiet, "generation and labels unchanged");
            source.Send(Modify(Pod(generation: 2)));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet);
            source.Send(Modify(Pod(generation: 2, labels: new Dictionary<string, string> { ["a"] = "b" })));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet);
        }

        [Fact]
        public async Task IgnoreWritesFrom_QuietsOnlyOurOwnVersions()
        {
            var writes = new OwnWrites();
            var source = Source(Pod());
            await using var e = source.IgnoreWritesFrom(writes).GetAsyncEnumerator(Ct);
            await NextFlags(e);

            writes.Record("prod/web", "2");
            source.Send(Modify(Pod(phase: "Running", resourceVersion: "2")));
            (await NextFlags(e)).Should().HaveFlag(EventTypeFlags.Quiet);
            writes.Count.Should().Be(0, "a seen echo is consumed");

            source.Send(Modify(Pod(phase: "Failed", resourceVersion: "3")));
            (await NextFlags(e)).Should().NotHaveFlag(EventTypeFlags.Quiet, "someone else's write");
        }

        [Fact]
        public async Task IgnoreWritesFrom_DeleteForgetsTheKey()
        {
            var writes = new OwnWrites();
            var source = Source(Pod());
            await using var e = source.IgnoreWritesFrom(writes).GetAsyncEnumerator(Ct);
            await NextFlags(e);
            writes.Record("prod/web", "5");
            source.Send(new ResourceEvent<string, V1Pod>(EventTypeFlags.Delete, "prod/web", Pod()));
            await NextFlags(e);
            writes.Count.Should().Be(0);
        }

        [Fact]
        public void OwnWrites_EntriesExpire()
        {
            var now = new DateTime(2026, 1, 1);
            var writes = new OwnWrites(TimeSpan.FromMinutes(1), () => now);
            writes.Record("a", "1");
            now = now.AddMinutes(2);
            writes.IsOwnWrite("a", "1").Should().BeFalse();
            writes.Record("b", "1");
            writes.Count.Should().Be(1, "expired entries are purged");
        }
    }
}
