using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>A controller the operator runs: compiled into informers, joins and <c>RunAsync</c>.</summary>
    internal interface IControllerRunner
    {
        string Name { get; }

        /// <summary>Completes once the controller's informers delivered their initial state.</summary>
        Task Synced { get; }

        /// <summary>The resources the controller reads and writes, for RBAC.</summary>
        IReadOnlyList<(Type Type, IReadOnlyList<string> Verbs)> Resources { get; }

        Task RunAsync(CancellationToken cancellationToken);
    }

    /// <summary>What the runner needs from the operator.</summary>
    internal sealed class ControllerServices
    {
        public IKubernetesInformerFactory Informers { get; init; }
        public IKubernetesGenericClient Client { get; init; }
        public IEventRecorder Events { get; init; }
        public ILoggerFactory LoggerFactory { get; init; }
        public string DefaultNamespace { get; init; }
        public ILeaderElection LeaderElection { get; init; }
        public bool AutomaticEvents { get; init; } = true;
    }

    internal sealed class ControllerRunner<T> : IControllerRunner where T : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly ControllerBuilder<T> _builder;
        private readonly ControllerServices _services;
        private readonly Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> _reconcile;
        private readonly Func<string, T, CancellationToken, Task> _deleted;
        private readonly ILogger _logger;
        private readonly Expectations _expectations;
        private readonly OwnWrites _ownWrites = new OwnWrites();
        private readonly IKubernetesGenericClient _client;
        private readonly TaskCompletionSource<bool> _synced = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        public ControllerRunner(
            ControllerBuilder<T> builder,
            ControllerServices services,
            Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> reconcile,
            Func<string, T, CancellationToken, Task> deleted)
        {
            _builder = builder;
            _services = services;
            _reconcile = reconcile ?? throw new InvalidOperationException($"Controller {builder.ControllerName} has no reconcile");
            _deleted = deleted;
            _logger = services.LoggerFactory?.CreateLogger($"Steeltoe.Informers.Operator.{builder.ControllerName}") ?? NullLogger.Instance;
            _expectations = new Expectations(builder.ExpectationTimeout);
            // every write is recorded (it's cheap); only IgnoreOwnWrites acts on it
            _client = services.Client.RecordingWritesTo(_ownWrites);
        }

        public string Name => _builder.ControllerName;

        public Task Synced => _synced.Task;

        public IReadOnlyList<(Type Type, IReadOnlyList<string> Verbs)> Resources =>
            new[] { (typeof(T), (IReadOnlyList<string>)new[] { "get", "list", "watch", "update", "patch" }) }
                .Concat(_builder.Relationships.Select(x => (x.OtherType, x.Verbs)))
                .ToList();

        private string Namespace => _builder.AllNamespacesScope ? null : _builder.NamespaceScope ?? _services.DefaultNamespace;

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var state = await BuildAsync(cancellationToken).ConfigureAwait(false);
            await state.RunAsync(HandleAsync, new RunOptions
            {
                Name = Name,
                MaxConcurrency = _builder.Concurrency,
                Backoff = _builder.RetryBackoff,
                RateLimit = _builder.StartRateLimit,
                LeaderElection = _services.LeaderElection,
                Logger = _logger,
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     The pipeline: the primary informer (scoped, filtered, own writes and trigger applied), one join per relationship, then an optional
        ///     resync.
        /// </summary>
        internal async Task<IInformable<string, ControllerState<T>>> BuildAsync(CancellationToken cancellationToken)
        {
            var ns = Namespace;
            IInformer<string, T> informer = _services.Informers.Get<T>();
            if (ns != null)
                informer = informer.Where(x => x.Metadata.NamespaceProperty == ns);
            foreach (var filter in _builder.Filters)
                informer = informer.Where(filter);
            IInformable<string, T> primary = informer;
            if (_builder.IgnoresOwnWrites)
                primary = primary.IgnoreWritesFrom(_ownWrites);
            if (_builder.PrimaryTrigger != null)
                primary = primary.TriggerOn(_builder.PrimaryTrigger);

            var relationships = _builder.Relationships.Count;
            var state = primary.Select(x => new ControllerState<T>(x, relationships));
            var (apiVersion, kind) = TypeMeta();
            var scope = new ControllerScope
            {
                Informers = _services.Informers,
                Client = _services.Client,
                Namespace = ns,
                OwnWrites = _builder.IgnoresOwnWrites ? _ownWrites : null,
                Expectations = _expectations,
                PrimaryApiVersion = apiVersion,
                PrimaryKind = kind,
            };
            foreach (var relationship in _builder.Relationships)
                state = await relationship.JoinAsync(state, scope, cancellationToken).ConfigureAwait(false);
            if (_builder.ResyncPeriod != null)
                state = state.Resync(_builder.ResyncPeriod.Value);
            return state.Do(e =>
            {
                if (e.EventFlags.HasFlag(EventTypeFlags.ResetEnd))
                    _synced.TrySetResult(true);
            });
        }

        private static (string ApiVersion, string Kind) TypeMeta()
        {
            var resource = GroupVersionResource.For<T>();
            var attribute = (KubernetesEntityAttribute)Attribute.GetCustomAttribute(typeof(T), typeof(KubernetesEntityAttribute));
            return (resource.ApiVersion, attribute?.Kind ?? typeof(T).Name);
        }

        internal async Task<ReconcileResult> HandleAsync(ResourceEvent<string, ControllerState<T>> e, CancellationToken cancellationToken)
        {
            if (e.EventFlags.HasFlag(EventTypeFlags.Delete))
            {
                _expectations.Delete(e.Key);
                _ownWrites.Forget(e.Key);
                if (_deleted != null)
                    await _deleted(e.Key, e.Value?.Primary, cancellationToken).ConfigureAwait(false);
                return ReconcileResult.Done;
            }

            var context = CreateContext(e.Key, RunContext.Trigger, e.Value);
            using var scope = _logger.BeginScope(new Dictionary<string, object> { ["Controller"] = Name, ["Key"] = e.Key });
            try
            {
                var result = await ReconcileWithFinalizersAsync(context, cancellationToken).ConfigureAwait(false);
                var unsaved = context.UnsavedChanges();
                if (unsaved > 0)
                    _logger.LogWarning("Reconciling {Key} edited {Count} object(s) without saving them; the edits are discarded", e.Key, unsaved);
                return result;
            }
            catch (Exception ex) when (!ex.IsConflict() && !(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                if (_services.AutomaticEvents)
                    context.Events.Warning("ReconcileError", ex.Message);
                throw;
            }
        }

        internal ReconcileContext<T> CreateContext(string key, ReconcileTrigger trigger, ControllerState<T> state) =>
            new ReconcileContext<T>(
                key,
                trigger,
                state.Primary,
                _builder.Relationships.Select(x => x.ToRelatedSet(state.Related(x.Index))),
                _client,
                _expectations,
                _services.Events,
                _logger);

        private async Task<ReconcileResult> ReconcileWithFinalizersAsync(ReconcileContext<T> context, CancellationToken cancellationToken)
        {
            var finalizers = _builder.Finalizers;
            var obj = context.Object.Original;
            if (finalizers.Count > 0 && obj.IsBeingDeleted())
            {
                foreach (var (name, cleanup) in finalizers)
                {
                    if (!context.Object.Original.HasFinalizer(name))
                        continue;
                    var result = await cleanup(context, cancellationToken).ConfigureAwait(false);
                    if (!result.IsDone)
                        return result;
                    var remaining = await _client.RemoveFinalizerAsync(context.Object.Original, name, cancellationToken).ConfigureAwait(false);
                    if (remaining == null)
                        break; // the deletion completed
                    context.Object.Accept(remaining);
                    if (_services.AutomaticEvents)
                        context.Events.Normal("Finalized", $"Finalizer {name} completed");
                }

                return ReconcileResult.Done;
            }

            if (finalizers.Any(x => !obj.HasFinalizer(x.Name)))
            {
                var updated = await _client.AddFinalizersAsync(obj, finalizers.Select(x => x.Name), cancellationToken).ConfigureAwait(false);
                context.Object.Accept(updated);
            }

            if (!_expectations.IsSatisfied(context.Key))
            {
                // earlier creates or deletes aren't all observed yet: their events re-run us (or the timeout does)
                return ReconcileResult.RequeueAfter(_expectations.RemainingTime(context.Key));
            }

            return await _reconcile(context, cancellationToken).ConfigureAwait(false);
        }
    }
}
