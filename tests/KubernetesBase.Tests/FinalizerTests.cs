using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AwesomeAssertions;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase.Tests.Fakes;
using Xunit;

namespace Steeltoe.Informers.KubernetesBase.Tests
{
    public class FinalizerTests
    {
        private static CancellationToken Ct => TestContext.Current.CancellationToken;

        private static ResourceEvent<string, V1ConfigMap> Modify(V1ConfigMap obj) => new ResourceEvent<string, V1ConfigMap>(EventTypeFlags.Modify, obj.ObjectKey(), obj);

        private static V1ConfigMap New(params string[] finalizers) => new V1ConfigMap
        {
            Metadata = new V1ObjectMeta { Name = "cm", NamespaceProperty = "prod", Finalizers = finalizers.Length == 0 ? null : new List<string>(finalizers) },
        };

        private readonly FakeGenericClient _client = new FakeGenericClient { HasStatus = false };
        private readonly List<string> _calls = new List<string>();

        private Func<ResourceEvent<string, V1ConfigMap>, CancellationToken, Task<ReconcileResult>> Handler(ReconcileResult first, ReconcileResult second) =>
            Finalizers.Handle<string, V1ConfigMap, V1ConfigMap>(_client, x => x, (e, _) =>
                {
                    _calls.Add("reconcile");
                    return Task.FromResult(ReconcileResult.Done);
                },
                ("example.com/first", (e, _) =>
                {
                    _calls.Add("first");
                    return Task.FromResult(first);
                }),
                ("example.com/second", (e, _) =>
                {
                    _calls.Add("second");
                    return Task.FromResult(second);
                }));

        [Fact]
        public async Task NotBeingDeleted_AddsMissingFinalizersBeforeReconcile()
        {
            var obj = _client.Put(New("other"));
            await Handler(ReconcileResult.Done, ReconcileResult.Done)(Modify(obj), Ct);
            _calls.Should().Equal("reconcile");
            var stored = await _client.GetAsync<V1ConfigMap>("cm", "prod", Ct);
            stored.Metadata.Finalizers.Should().Equal("other", "example.com/first", "example.com/second");

            await Handler(ReconcileResult.Done, ReconcileResult.Done)(Modify(stored), Ct);
            _client.Writes.Should().HaveCount(1, "nothing to add the second time");
        }

        [Fact]
        public async Task BeingDeleted_RunsCleanupsInOrder_DoneRemovesEach()
        {
            var obj = New("example.com/first", "example.com/second", "other");
            obj.Metadata.DeletionTimestamp = DateTime.UtcNow;
            obj = _client.Put(obj);
            var result = await Handler(ReconcileResult.Done, ReconcileResult.Done)(Modify(obj), Ct);
            result.IsDone.Should().BeTrue();
            _calls.Should().Equal("first", "second");
            (await _client.GetAsync<V1ConfigMap>("cm", "prod", Ct)).Metadata.Finalizers.Should().Equal("other");
        }

        [Fact]
        public async Task BeingDeleted_CleanupNotDone_KeepsItsFinalizerAndStops()
        {
            var obj = New("example.com/first", "example.com/second");
            obj.Metadata.DeletionTimestamp = DateTime.UtcNow;
            obj = _client.Put(obj);
            var result = await Handler(ReconcileResult.Requeue(), ReconcileResult.Done)(Modify(obj), Ct);
            result.IsDone.Should().BeFalse();
            _calls.Should().Equal("first");
            (await _client.GetAsync<V1ConfigMap>("cm", "prod", Ct)).Metadata.Finalizers.Should().Equal("example.com/first", "example.com/second");
        }

        [Fact]
        public async Task BeingDeleted_WithoutOurFinalizers_CleansNothing()
        {
            var obj = New("other");
            obj.Metadata.DeletionTimestamp = DateTime.UtcNow;
            obj = _client.Put(obj);
            (await Handler(ReconcileResult.Done, ReconcileResult.Done)(Modify(obj), Ct)).IsDone.Should().BeTrue();
            _calls.Should().BeEmpty();
            _client.Writes.Should().BeEmpty();
        }

        [Fact]
        public async Task RemovingTheLastFinalizer_ObjectGone_CountsAsSuccess()
        {
            var obj = New("example.com/first");
            obj.Metadata.DeletionTimestamp = DateTime.UtcNow;
            obj = _client.Put(obj);
            await _client.DeleteAsync(obj, cancellationToken: Ct); // the API server deleted it as the finalizer went
            (await _client.RemoveFinalizerAsync(obj, "example.com/first", Ct)).Should().BeNull();
        }
    }
}
