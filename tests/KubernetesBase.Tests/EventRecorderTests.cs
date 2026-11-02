using System;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using NSubstitute;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class EventRecorderTests
    {
        private static readonly V1Pod Pod = new V1Pod { Metadata = new V1ObjectMeta { Name = "web", NamespaceProperty = "prod", Uid = "u1" } };

        private readonly FakeGenericClient _client = new FakeGenericClient { HasStatus = false };
        private DateTime _now = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private KubernetesEventRecorder Recorder(KubernetesEventRecorderOptions options = null) =>
            new KubernetesEventRecorder(_client, options ?? new KubernetesEventRecorderOptions { ReportingController = "example.com/test", ReportingInstance = "pod-a" }, null, () => _now);

        [Fact]
        public async Task Records_EventsV1_AboutTheObject()
        {
            await using var recorder = Recorder();
            recorder.For(Pod).Warning("FailedCreate", "quota exceeded", action: "Create");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            var events = await _client.ListAsync<Eventsv1Event>(cancellationToken: TestContext.Current.CancellationToken);
            var e = events.Single();
            e.Metadata.NamespaceProperty.Should().Be("prod");
            e.Metadata.Name.Should().StartWith("web.");
            e.Regarding.Should().BeEquivalentTo(new V1ObjectReference { ApiVersion = "v1", Kind = "Pod", Name = "web", NamespaceProperty = "prod", Uid = "u1" });
            e.Type.Should().Be("Warning");
            e.Reason.Should().Be("FailedCreate");
            e.Note.Should().Be("quota exceeded");
            e.Action.Should().Be("Create");
            e.ReportingController.Should().Be("example.com/test");
            e.ReportingInstance.Should().Be("pod-a");
        }

        [Fact]
        public async Task Repeats_WithinTheWindow_UpdateOneSeries()
        {
            await using var recorder = Recorder();
            for (var i = 0; i < 3; i++)
            {
                recorder.Normal(Pod, "Synced", "all good");
                _now = _now.AddSeconds(1);
            }

            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            _client.Writes.Select(x => x.Verb).Should().Equal("create", "patch", "patch");
            var e = (await _client.ListAsync<Eventsv1Event>(cancellationToken: TestContext.Current.CancellationToken)).Single();
            e.Series.Count.Should().Be(3);

            _now = _now.AddMinutes(11);
            recorder.Normal(Pod, "Synced", "all good");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            _client.Writes.Last().Verb.Should().Be("create", "outside the window a new event starts");
        }

        [Fact]
        public async Task EachObject_IsRateLimited()
        {
            await using var recorder = Recorder(new KubernetesEventRecorderOptions { BurstPerObject = 2, RefillInterval = TimeSpan.FromMinutes(1) });
            for (var i = 0; i < 5; i++)
                recorder.Normal(Pod, "Changed", $"change {i}");
            recorder.Normal(new V1Pod { Metadata = new V1ObjectMeta { Name = "other", NamespaceProperty = "prod" } }, "Changed", "x");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            _client.Writes.Should().HaveCount(3, "two for web, one for the other pod");

            _now = _now.AddMinutes(1);
            recorder.Normal(Pod, "Changed", "after refill");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            _client.Writes.Should().HaveCount(4);
        }

        [Fact]
        public async Task CoreV1_WritesCoreEvents()
        {
            await using var recorder = Recorder(new KubernetesEventRecorderOptions { Api = EventApi.CoreV1, ReportingController = "example.com/test" });
            recorder.Normal(Pod, "Created", "made it");
            recorder.Normal(Pod, "Created", "made it");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
            var e = (await _client.ListAsync<Corev1Event>(cancellationToken: TestContext.Current.CancellationToken)).Single();
            e.InvolvedObject.Name.Should().Be("web");
            e.Message.Should().Be("made it");
            e.Count.Should().Be(2);
            e.Source.Component.Should().Be("example.com/test");
        }

        [Fact]
        public async Task ApiFailures_AreDropped_NeverThrown()
        {
            var failing = Substitute.For<IKubernetesGenericClient>();
            failing.CreateAsync(Arg.Any<Eventsv1Event>(), Arg.Any<KubernetesWriteOptions>(), Arg.Any<System.Threading.CancellationToken>())
                .Returns<Task<Eventsv1Event>>(_ => throw new InvalidOperationException("forbidden"));
            await using var recorder = new KubernetesEventRecorder(failing);
            recorder.Normal(Pod, "Created", "x");
            await recorder.WhenIdleAsync().TimeoutIfNotDebugging();
        }
    }
}
