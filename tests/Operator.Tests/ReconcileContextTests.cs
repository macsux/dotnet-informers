using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.Samples.Controllers;
using Xunit;

namespace Steeltoe.Informers.Operator.Tests
{
    /// <summary>Reconciles tested without informers or an API server (R5.12), on the AppSet sample controller.</summary>
    public class ReconcileContextTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static AppSet Set(int replicas) => new AppSet
        {
            Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "demo", Uid = "set-uid", Generation = 3, ResourceVersion = "7" },
            Spec = new AppSetSpec { Replicas = replicas },
        };

        private static V1Pod Pod(string name, string phase, AppSet owner) => new V1Pod
        {
            Metadata = new V1ObjectMeta { Name = name, NamespaceProperty = "demo", CreationTimestamp = DateTime.UtcNow },
            Status = new V1PodStatus { Phase = phase },
        }.WithOwnerReference(owner);

        [Fact]
        public async Task MissingPods_AreCreatedOwned_AndStatusIsPatched()
        {
            var set = Set(2);
            var context = ReconcileContext.ForTest(set).WithOwned(Pod("web-a", "Running", set));
            var stats = new ControllerStats();
            var result = await new AppSetController(stats).ReconcileAsync(context, Ct);

            result.IsDone.Should().BeTrue("the created pod's events re-run the reconcile");
            var create = context.Writes.Should().ContainSingle(w => w.Verb == "create").Which;
            create.Kind.Should().Be("Pod");
            create.Name.Should().StartWith("web-");
            create.Body.Should().Contain("\"uid\":\"set-uid\"").And.Contain("\"controller\":true");
            var status = context.Writes.Should().ContainSingle(w => w.Verb == "patch").Which;
            status.Status.Should().BeTrue("status goes to the status subresource");
            status.Body.Should().Contain("\"replicas\":1").And.Contain("\"readyReplicas\":1").And.Contain("\"resourceVersion\":\"7\"");
            context.ExpectationsSatisfied.Should().BeFalse("the created pod isn't observed yet");
        }

        [Fact]
        public async Task Converged_WritesNothing()
        {
            var set = Set(1);
            set.Status = new AppSetStatus { Replicas = 1, ReadyReplicas = 1, ObservedGeneration = 3 };
            var context = ReconcileContext.ForTest(set).WithOwned(Pod("web-a", "Running", set));
            (await new AppSetController(new ControllerStats()).ReconcileAsync(context, Ct)).IsDone.Should().BeTrue();
            context.Writes.Should().BeEmpty();
        }

        [Fact]
        public async Task ExtraPods_AreDeletedNewestFirst()
        {
            var set = Set(1);
            var older = Pod("web-old", "Running", set);
            older.Metadata.CreationTimestamp = DateTime.UtcNow.AddMinutes(-5);
            var context = ReconcileContext.ForTest(set).WithOwned(older, Pod("web-new", "Running", set));
            await new AppSetController(new ControllerStats()).ReconcileAsync(context, Ct);
            context.Writes.Should().ContainSingle(w => w.Verb == "delete").Which.Name.Should().Be("web-new");
        }

        [Fact]
        public void Relationships_NotDeclared_AreReportedClearly()
        {
            var context = ReconcileContext.ForTest(Set(1));
            ((Action)(() => context.Owned<V1Pod>())).Should().Throw<InvalidOperationException>().WithMessage("*Owns<V1Pod>()*");
            ((Action)(() => context.Related<V1Secret>())).Should().Throw<InvalidOperationException>().WithMessage("*References<V1Secret>()*");
        }

        [Fact]
        public async Task SaveAsync_SavesEditedRelatedObjectsToo()
        {
            var set = Set(1);
            var secret = new V1Secret { Metadata = new V1ObjectMeta { Name = "creds", NamespaceProperty = "demo" } };
            var context = ReconcileContext.ForTest(set).WithRelated("db", secret);
            var tracked = context.Related<V1Secret>("db").Single();
            (tracked.Edit().Metadata.Labels ??= new Dictionary<string, string>())["used-by"] = "web";
            await context.SaveAsync(Ct);
            context.Writes.Should().ContainSingle().Which.Should().Match<RecordedWrite>(w => w.Verb == "patch" && w.Kind == "Secret" && w.Body.Contains("used-by"));
            tracked.Original.Metadata.Labels["used-by"].Should().Be("web", "the saved object becomes the handle's original");
            tracked.IsModified.Should().BeFalse();
        }
    }
}
