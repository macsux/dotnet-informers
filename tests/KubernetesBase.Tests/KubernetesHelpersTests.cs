using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class KubernetesHelpersTests
    {
        private static V1Pod OwnedPod(string name, params (string Kind, string Name, string Uid)[] owners) => new V1Pod
        {
            Metadata = new V1ObjectMeta
            {
                NamespaceProperty = "default",
                Name = name,
                OwnerReferences = owners.Select(x => new V1OwnerReference { Kind = x.Kind, Name = x.Name, Uid = x.Uid, ApiVersion = "apps/v1" }).ToList(),
            },
        };

        [Fact]
        public void ObjectKey_NamespaceAndName_OrNameWhenClusterScoped()
        {
            new V1Pod { Metadata = new V1ObjectMeta { NamespaceProperty = "prod", Name = "web" } }.ObjectKey().Should().Be("prod/web");
            new V1Node { Metadata = new V1ObjectMeta { Name = "n1" } }.ObjectKey().Should().Be("n1");
        }

        [Fact]
        public void OwnerHelpers_MatchByUid()
        {
            var rs = new V1ReplicaSet { Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "web", Uid = "rs-uid" } };
            var deployment = new V1Deployment { Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "web", Uid = "dep-uid" } };
            var pod = OwnedPod("p", ("ReplicaSet", "web", "rs-uid"));
            pod.OwnedBy(rs).Should().BeTrue();
            pod.OwnedBy(deployment).Should().BeFalse("same name, different object");
            pod.OwnerUids().Should().Equal("rs-uid");
            pod.OwnerUids("Deployment").Should().BeEmpty();
            pod.OwnerKeys("ReplicaSet").Should().Equal("default/web");
        }

        [Fact]
        public async Task Join_OnOwnerUid_DoesNotAdoptChildrenOfARecreatedOwner()
        {
            var api = new FakeApiServer();
            var replicaSets = GroupVersionResource.For<V1ReplicaSet>();
            var pods = GroupVersionResource.For<V1Pod>();
            api.Put(replicaSets, new V1ReplicaSet { ApiVersion = "apps/v1", Kind = "ReplicaSet", Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "web", Uid = "new-uid" } });
            api.Put(pods, OwnedPod("orphan", ("ReplicaSet", "web", "old-uid")));
            api.Put(pods, OwnedPod("child", ("ReplicaSet", "web", "new-uid")));
            var factory = new KubernetesInformerFactory(api, retryPolicy: RetryPolicy.None);
            var join = factory.Get<V1ReplicaSet>().Join(factory.Get<V1Pod>(), rs => rs.Metadata.Uid, p => p.OwnerUids(), (rs, p) => p.Metadata.Name);
            var result = await join.ToList();
            result.Should().Equal("child");
        }

        [Fact]
        public void IsBeingDeleted()
        {
            new V1Pod { Metadata = new V1ObjectMeta() }.IsBeingDeleted().Should().BeFalse();
            new V1Pod { Metadata = new V1ObjectMeta { DeletionTimestamp = DateTime.UtcNow } }.IsBeingDeleted().Should().BeTrue();
        }

        [Fact]
        public void DeepClone_IsIndependent()
        {
            var pod = new V1Pod { Metadata = new V1ObjectMeta { Name = "a", Labels = new Dictionary<string, string> { ["x"] = "1" } } };
            var clone = pod.DeepClone();
            clone.Metadata.Labels["x"] = "2";
            pod.Metadata.Labels["x"].Should().Be("1");
        }

        [Fact]
        public void Expectations_SatisfiedWhenObservedOrTimedOut()
        {
            var now = DateTime.UtcNow;
            var sut = new Expectations(TimeSpan.FromMinutes(5), () => now);
            sut.IsSatisfied("default/set").Should().BeTrue();
            sut.ExpectCreations("default/set", 2);
            sut.IsSatisfied("default/set").Should().BeFalse();
            sut.CreationObserved("default/set");
            sut.IsSatisfied("default/set").Should().BeFalse();
            sut.CreationFailed("default/set");
            sut.IsSatisfied("default/set").Should().BeTrue();

            sut.ExpectDeletions("default/set", 1);
            sut.IsSatisfied("default/set").Should().BeFalse();
            sut.RemainingTime("default/set").Should().Be(TimeSpan.FromMinutes(5));
            now += TimeSpan.FromMinutes(5);
            sut.IsSatisfied("default/set").Should().BeTrue("the expectation timed out");
        }

        [Fact]
        public async Task Expectations_Observe_CountsBeforeDownstreamSeesTheEvent()
        {
            var sut = new Expectations(TimeSpan.FromMinutes(5));
            sut.ExpectCreations("default/web", 2);
            var source = Informable.Create(new[] { OwnedPod("a", ("AppSet", "web", "u")) }, x => x.ObjectKey(),
                new[] { OwnedPod("b", ("AppSet", "web", "u")).ToResourceEvent(EventTypeFlags.Add) });
            var satisfiedAtEachEvent = new List<bool>();
            await foreach (var _ in sut.Observe(source, p => p.OwnerKeys("AppSet")).WithCancellation(TestContext.Current.CancellationToken))
                satisfiedAtEachEvent.Add(sut.IsSatisfied("default/web"));
            satisfiedAtEachEvent.Should().Equal(false, true);
        }

        [Fact]
        public async Task MutationDetector_FailsTheInformer_WhenAConsumerModifiesASharedObject()
        {
            var api = new FakeApiServer();
            var pods = GroupVersionResource.For<V1Pod>();
            api.Put(pods, new V1Pod { ApiVersion = "v1", Kind = "Pod", Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "a", Labels = new Dictionary<string, string>() } });
            var factory = new KubernetesInformerFactory(api, new[] { new KubernetesInformerRegistration<V1Pod>().DetectMutations() }, retryPolicy: RetryPolicy.None);
            await using var e = factory.Get<V1Pod>().GetAsyncEnumerator(TestContext.Current.CancellationToken);
            await e.MoveNextAsync();
            e.Current.Value.Metadata.Labels["oops"] = "mutated"; // breaking the contract
            for (var i = 0; i < 100 && api.OpenWatches == 0; i++)
                await Task.Delay(10, TestContext.Current.CancellationToken);
            api.Put(pods, new V1Pod { ApiVersion = "v1", Kind = "Pod", Metadata = new V1ObjectMeta { NamespaceProperty = "default", Name = "a" } });
            var act = async () => await e.MoveNextAsync();
            (await act.Should().ThrowAsync<InformerException>()).WithInnerException<InvalidOperationException>();
        }
    }
}
