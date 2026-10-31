using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class PatchingTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static V1Pod NewPod() => new V1Pod
        {
            ApiVersion = "v1",
            Kind = "Pod",
            Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod", Labels = new Dictionary<string, string> { ["app"] = "web" }, Finalizers = new List<string> { "a" } },
            Spec = new V1PodSpec { Containers = new List<V1Container> { new V1Container { Name = "app", Image = "nginx" } } },
            Status = new V1PodStatus { Phase = "Pending" },
        };

        [Fact]
        public void MergePatch_ContainsOnlyTheDifference()
        {
            var before = JsonNode.Parse("{\"a\":1,\"b\":{\"c\":2,\"d\":3},\"e\":[1,2],\"f\":\"x\"}")!.AsObject();
            var after = JsonNode.Parse("{\"a\":1,\"b\":{\"c\":5,\"d\":3},\"e\":[1,2,3],\"g\":true}")!.AsObject();
            JsonMergePatch.Create(before, after)!.ToJsonString().Should().Be("{\"b\":{\"c\":5},\"e\":[1,2,3],\"g\":true,\"f\":null}");
            JsonMergePatch.Create(before, before.DeepClone().AsObject()).Should().BeNull();
        }

        [Fact]
        public async Task NoChange_SendsNothing()
        {
            var client = new FakeGenericClient();
            var pod = client.Put(NewPod());
            var result = await client.PatchAsync(pod, p => p.Metadata.Labels["app"] = "web", cancellationToken: Ct);
            result.Should().BeSameAs(pod);
            client.Writes.Should().BeEmpty();
        }

        [Fact]
        public async Task MetadataChange_IsAMergePatchGuardedByResourceVersion_AndLeavesTheOriginalAlone()
        {
            var client = new FakeGenericClient();
            var pod = client.Put(NewPod());
            var result = await client.PatchAsync(pod, p => p.Metadata.Labels["tier"] = "front", cancellationToken: Ct);
            result.Metadata.Labels["tier"].Should().Be("front");
            pod.Metadata.Labels.Should().NotContainKey("tier", "the original (possibly shared by an informer) is never modified");
            var write = client.Writes.Single();
            write.Status.Should().BeFalse();
            write.Body.Should().Be($"{{\"metadata\":{{\"labels\":{{\"tier\":\"front\"}},\"resourceVersion\":\"{pod.Metadata.ResourceVersion}\"}}}}");
        }

        [Fact]
        public async Task StatusAndSpecChanges_AreSplitBetweenTheEndpoints()
        {
            var client = new FakeGenericClient();
            var pod = client.Put(NewPod());
            var result = await client.PatchAsync(pod, p =>
            {
                p.Status.Phase = "Running";
                p.Metadata.Finalizers.Clear();
            }, cancellationToken: Ct);
            client.Writes.Select(x => x.Status).Should().Equal(false, true);
            client.Writes[0].Body.Should().NotContain("status");
            client.Writes[1].Body.Should().StartWith("{\"status\":{\"phase\":\"Running\"}");
            result.Status.Phase.Should().Be("Running");
            result.Metadata.Finalizers.Should().BeEmpty();
        }

        [Fact]
        public async Task RemovingStatus_IsSentToTheStatusSubresource()
        {
            var client = new FakeGenericClient();
            var pod = client.Put(NewPod());
            var result = await client.PatchAsync(pod, p => p.Status = null, cancellationToken: Ct);
            client.Writes.Single().Should().Match<(string Verb, string Key, bool Status, string Body)>(w => w.Status && w.Body.StartsWith("{\"status\":null"));
            result.Status.Should().BeNull();
        }

        [Fact]
        public async Task WithoutStatusSubresource_StatusGoesToTheMainResource()
        {
            var client = new FakeGenericClient { HasStatus = false };
            var pod = client.Put(NewPod());
            await client.PatchAsync(pod, p => p.Status.Phase = "Running", cancellationToken: Ct);
            client.Writes.Single().Status.Should().BeFalse();
            client.Writes.Single().Body.Should().Contain("\"status\":{\"phase\":\"Running\"}");
        }

        [Fact]
        public async Task StaleOriginal_Conflicts_UnlessTheCallerOptsOut()
        {
            var client = new FakeGenericClient();
            var stale = client.Put(NewPod());
            client.Put(NewPod()); // someone else wrote since
            var act = () => client.PatchAsync(stale, p => p.Metadata.Labels["x"] = "y", cancellationToken: Ct);
            (await act.Should().ThrowAsync<Exception>()).Which.IsConflict().Should().BeTrue();

            var forced = await client.PatchAsync(stale, p => p.Metadata.Labels["x"] = "y", new PatchOptions { RequireCurrentVersion = false }, Ct);
            forced.Metadata.Labels["x"].Should().Be("y");
        }

        [Fact]
        public async Task Tracked_EditsACopy_SaveSendsTheDiffAndAcceptsTheResult()
        {
            var client = new FakeGenericClient();
            var original = client.Put(NewPod());
            var tracked = Tracked.Of(original);
            tracked.Value.Should().BeSameAs(original);
            tracked.IsModified.Should().BeFalse();

            var edit = tracked.Edit();
            edit.Should().NotBeSameAs(original);
            tracked.Edit().Should().BeSameAs(edit, "one clone per handle");
            tracked.Value.Should().BeSameAs(edit);
            edit.Status.Phase = "Running";
            tracked.HasChanges().Should().BeTrue();
            original.Status.Phase.Should().Be("Pending");

            var saved = await client.SaveAsync(tracked, cancellationToken: Ct);
            tracked.Original.Should().BeSameAs(saved);
            tracked.IsModified.Should().BeFalse();
            saved.Status.Phase.Should().Be("Running");

            tracked.Edit().Status.Phase = "Running";
            tracked.HasChanges().Should().BeFalse("an edit back to the same value is no change");
            await client.SaveAsync(tracked, cancellationToken: Ct);
            client.Writes.Should().HaveCount(1);
        }

        [Fact]
        public void OwnerReference_FromOwner_RejectsUncreatedAndCrossNamespaceOwners()
        {
            var owner = new V1Deployment { Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod", Uid = "u1" } };
            var pod = new V1Pod { Metadata = new V1ObjectMeta { GenerateName = "web-" } }.WithOwnerReference(owner);
            pod.Metadata.NamespaceProperty.Should().Be("prod", "a child without a namespace takes the owner's");
            var reference = pod.Metadata.OwnerReferences.Single();
            reference.Should().BeEquivalentTo(new V1OwnerReference { ApiVersion = "apps/v1", Kind = "Deployment", Name = "web", Uid = "u1", Controller = true, BlockOwnerDeletion = true });

            pod.WithOwnerReference(owner);
            pod.Metadata.OwnerReferences.Should().HaveCount(1, "no duplicates");

            var uncreated = new V1Deployment { Metadata = new V1ObjectMeta { Name = "new", NamespaceProperty = "prod" } };
            ((Action)(() => new V1Pod().WithOwnerReference(uncreated))).Should().Throw<InvalidOperationException>();
            ((Action)(() => new V1Pod { Metadata = new V1ObjectMeta { NamespaceProperty = "other" } }.WithOwnerReference(owner))).Should().Throw<InvalidOperationException>();
            var otherController = new V1ReplicaSet { Metadata = new V1ObjectMeta { Name = "rs", NamespaceProperty = "prod", Uid = "u2" } };
            ((Action)(() => pod.WithOwnerReference(otherController))).Should().Throw<InvalidOperationException>("one controller per object");
            pod.WithOwnerReference(otherController, controller: false).Metadata.OwnerReferences.Should().HaveCount(2);
        }

        [Fact]
        public async Task CreateAndDeleteOwned_RecordExpectations()
        {
            var client = new FakeGenericClient();
            var owner = client.Put(new V1Deployment { Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod" } });
            var expectations = new Expectations(TimeSpan.FromMinutes(1));
            var pod = await client.CreateOwnedAsync(owner, new V1Pod { Metadata = new V1ObjectMeta { GenerateName = "web-" } }, expectations, cancellationToken: Ct);
            pod.Metadata.OwnerReferences.Single().Uid.Should().Be(owner.Metadata.Uid);
            expectations.IsSatisfied("prod/web").Should().BeFalse("the create isn't observed yet");
            expectations.CreationObserved("prod/web");
            expectations.IsSatisfied("prod/web").Should().BeTrue();

            (await client.DeleteOwnedAsync(owner, pod, expectations, cancellationToken: Ct)).Should().BeTrue();
            expectations.IsSatisfied("prod/web").Should().BeFalse();
            (await client.DeleteOwnedAsync(owner, pod, expectations, cancellationToken: Ct)).Should().BeFalse("already gone");
            expectations.DeletionObserved("prod/web");
            expectations.IsSatisfied("prod/web").Should().BeTrue("a delete of something already gone is never observed, so it isn't expected");
        }

        [Fact]
        public void Conditions_TransitionTimeChangesOnlyWithStatus()
        {
            var conditions = new List<V1Condition>();
            var t0 = new DateTime(2026, 1, 1, 0, 0, 0, 500, DateTimeKind.Utc);
            conditions.Set("Ready", false, "Starting", observedGeneration: 1, now: t0);
            conditions.Get("Ready").LastTransitionTime.Should().Be(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), "truncated to what the API stores");
            conditions.Set("Ready", false, "StillStarting", observedGeneration: 2, now: t0.AddMinutes(1));
            conditions.Get("Ready").LastTransitionTime.Should().Be(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            conditions.Get("Ready").Reason.Should().Be("StillStarting");
            conditions.Get("Ready").ObservedGeneration.Should().Be(2);
            conditions.Set("Ready", true, "PodsReady", now: t0.AddMinutes(2));
            conditions.IsTrue("Ready").Should().BeTrue();
            conditions.Get("Ready").LastTransitionTime.Should().Be(new DateTime(2026, 1, 1, 0, 2, 0, DateTimeKind.Utc));
            conditions.Should().HaveCount(1);
            conditions.Remove("Ready").Should().BeTrue();
            conditions.Should().BeEmpty();
        }
    }
}
