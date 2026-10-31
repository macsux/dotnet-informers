using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>Declares what a controller watches and how it runs (see <see cref="Controller{T}.Configure" />).</summary>
    public sealed class ControllerBuilder<T> where T : class, IKubernetesObject<V1ObjectMeta>
    {
        internal ControllerBuilder(string name) => ControllerName = name;

        internal string ControllerName { get; private set; }
        internal string NamespaceScope { get; private set; }
        internal bool AllNamespacesScope { get; private set; }
        internal List<Expression<Func<T, bool>>> Filters { get; } = new List<Expression<Func<T, bool>>>();
        internal ITrigger<T> PrimaryTrigger { get; private set; }
        internal bool IgnoresOwnWrites { get; private set; }
        internal List<Relationship<T>> Relationships { get; } = new List<Relationship<T>>();
        internal List<(string Name, Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> Cleanup)> Finalizers { get; } =
            new List<(string, Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>>)>();
        internal int Concurrency { get; private set; } = 1;
        internal Backoff RetryBackoff { get; private set; }
        internal RateLimit StartRateLimit { get; private set; }
        internal TimeSpan? ResyncPeriod { get; private set; }
        internal TimeSpan ExpectationTimeout { get; private set; } = TimeSpan.FromMinutes(5);
        internal Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> ReconcileDelegate { get; private set; }
        internal Func<string, T, CancellationToken, Task> DeletedDelegate { get; private set; }

        /// <summary>The name in logs, metrics, traces and events (default: the controller's type name).</summary>
        public ControllerBuilder<T> Named(string name)
        {
            ControllerName = string.IsNullOrEmpty(name) ? throw new ArgumentNullException(nameof(name)) : name;
            return this;
        }

        /// <summary>Watches one namespace (instead of the operator's default). Related objects are watched in the same namespace.</summary>
        public ControllerBuilder<T> Namespace(string @namespace)
        {
            NamespaceScope = string.IsNullOrEmpty(@namespace) ? throw new ArgumentNullException(nameof(@namespace)) : @namespace;
            AllNamespacesScope = false;
            return this;
        }

        /// <summary>Watches every namespace, whatever the operator's default.</summary>
        public ControllerBuilder<T> AllNamespaces()
        {
            NamespaceScope = null;
            AllNamespacesScope = true;
            return this;
        }

        /// <summary>Only reconciles objects matching <paramref name="predicate" /> (pushed down to the API server where it can be, like <c>Where</c> on an informer).</summary>
        public ControllerBuilder<T> Where(Expression<Func<T, bool>> predicate)
        {
            Filters.Add(predicate ?? throw new ArgumentNullException(nameof(predicate)));
            return this;
        }

        /// <summary>
        ///     Reconciles on changes to the selected values of the primary only (plus adds, deletes, a deletion starting and resyncs); other changes
        ///     still update its state. Without it every change triggers.
        /// </summary>
        public ControllerBuilder<T> TriggerOn(params Expression<Func<T, object>>[] selectors) => TriggerOn(Trigger.On(selectors));

        /// <summary>Reconciles on <paramref name="trigger" /> (e.g. <c>Trigger.AllButStatus</c>) or the selected values changing.</summary>
        public ControllerBuilder<T> TriggerOn(ITrigger<T> trigger, params Expression<Func<T, object>>[] selectors)
        {
            if (trigger == null)
                throw new ArgumentNullException(nameof(trigger));
            PrimaryTrigger = selectors == null || selectors.Length == 0 ? trigger : Trigger.Combine(trigger, Trigger.On(selectors));
            return this;
        }

        /// <summary>
        ///     Doesn't reconcile for the echoes of the controller's own writes (saves, creates, finalizer patches) on the primary and related
        ///     objects; any other writer's change still triggers.
        /// </summary>
        public ControllerBuilder<T> IgnoreOwnWrites()
        {
            IgnoresOwnWrites = true;
            return this;
        }

        /// <summary>
        ///     Objects the controller creates and that belong to the primary through a controller owner reference (e.g. an AppSet's Pods). A
        ///     change to one re-runs its owner; creates and deletes through <see cref="ReconcileContext{T}.CreateOwnedAsync{TChild}" /> are tracked
        ///     with expectations, so the reconcile waits until they're observed.
        /// </summary>
        public ControllerBuilder<T> Owns<TChild>(ITrigger<TChild> trigger = null) where TChild : class, IKubernetesObject<V1ObjectMeta>
        {
            if (Relationships.Any(x => x.Kind == RelationshipKind.Owns && x.OtherType == typeof(TChild)))
                throw new InvalidOperationException($"Owns<{typeof(TChild).Name}>() is declared twice");
            Relationships.Add(new OwnsRelationship<T, TChild>(Relationships.Count, trigger));
            return this;
        }

        /// <summary><see cref="Owns{TChild}(ITrigger{TChild})" /> re-running the owner only when the selected values of a child change.</summary>
        public ControllerBuilder<T> Owns<TChild>(Expression<Func<TChild, object>> trigger) where TChild : class, IKubernetesObject<V1ObjectMeta> =>
            Owns(Trigger.On(trigger ?? throw new ArgumentNullException(nameof(trigger))));

        /// <summary>
        ///     Objects the primary points at by name (e.g. <c>spec.secretRef</c>). <paramref name="names" /> returns them as names in the primary's
        ///     namespace, <c>namespace/name</c> for another namespace, or plain names of cluster-scoped objects. The referenced object appearing,
        ///     changing or disappearing re-runs every primary that references it.
        /// </summary>
        /// <param name="names">The referenced names (null entries are ignored)</param>
        /// <param name="name">Distinguishes two references of the same type (<c>Related&lt;V1Secret&gt;("tls")</c>)</param>
        /// <param name="trigger">Which changes of a referenced object re-run (default: every change)</param>
        public ControllerBuilder<T> References<TOther>(Func<T, IEnumerable<string>> names, string name = null, ITrigger<TOther> trigger = null)
            where TOther : class, IKubernetesObject<V1ObjectMeta>
        {
            Relationships.Add(new ReferencesRelationship<T, TOther>(Relationships.Count, name, names ?? throw new ArgumentNullException(nameof(names)), trigger));
            return this;
        }

        /// <summary>
        ///     Objects that point at primaries some other way (a label, a field): <paramref name="primaryKeys" /> returns, for one of them, the keys
        ///     (<c>namespace/name</c>) of the primaries it affects.
        /// </summary>
        public ControllerBuilder<T> Watches<TOther>(Func<TOther, IEnumerable<string>> primaryKeys, string name = null, ITrigger<TOther> trigger = null)
            where TOther : class, IKubernetesObject<V1ObjectMeta>
        {
            Relationships.Add(new WatchesRelationship<T, TOther>(Relationships.Count, name, primaryKeys ?? throw new ArgumentNullException(nameof(primaryKeys)), trigger));
            return this;
        }

        /// <summary>
        ///     A finalizer: added before the first reconcile of an object that isn't being deleted. Once deletion starts, <paramref name="cleanup" />
        ///     runs instead of the reconcile (finalizers in declaration order); <c>Done</c> removes it, anything else retries with backoff.
        /// </summary>
        public ControllerBuilder<T> Finalizer(string name, Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> cleanup)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));
            if (Finalizers.Any(x => x.Name == name))
                throw new InvalidOperationException($"Finalizer {name} is declared twice");
            Finalizers.Add((name, cleanup ?? throw new ArgumentNullException(nameof(cleanup))));
            return this;
        }

        /// <summary>How many objects reconcile at once (default 1). One object never reconciles twice at once.</summary>
        public ControllerBuilder<T> MaxConcurrency(int concurrency)
        {
            Concurrency = concurrency >= 1 ? concurrency : throw new ArgumentOutOfRangeException(nameof(concurrency));
            return this;
        }

        /// <summary>The per-object backoff of retries.</summary>
        public ControllerBuilder<T> Backoff(Backoff backoff)
        {
            RetryBackoff = backoff ?? throw new ArgumentNullException(nameof(backoff));
            return this;
        }

        /// <summary>An overall limit on how fast reconciles start.</summary>
        public ControllerBuilder<T> RateLimit(RateLimit rateLimit)
        {
            StartRateLimit = rateLimit ?? throw new ArgumentNullException(nameof(rateLimit));
            return this;
        }

        /// <summary>Re-reconciles every object periodically, without changes.</summary>
        public ControllerBuilder<T> Resync(TimeSpan period)
        {
            ResyncPeriod = period > TimeSpan.Zero ? period : throw new ArgumentOutOfRangeException(nameof(period));
            return this;
        }

        /// <summary>How long an unobserved create or delete of an owned object holds the reconcile back (default 5 minutes).</summary>
        public ControllerBuilder<T> ExpectationsTimeout(TimeSpan timeout)
        {
            ExpectationTimeout = timeout > TimeSpan.Zero ? timeout : throw new ArgumentOutOfRangeException(nameof(timeout));
            return this;
        }

        /// <summary>The reconcile of a controller declared without a class.</summary>
        public ControllerBuilder<T> Reconcile(Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>> reconcile)
        {
            ReconcileDelegate = reconcile ?? throw new ArgumentNullException(nameof(reconcile));
            return this;
        }

        /// <summary>What a controller declared without a class does once an object is gone (see <see cref="Controller{T}.DeletedAsync" />).</summary>
        public ControllerBuilder<T> Deleted(Func<string, T, CancellationToken, Task> deleted)
        {
            DeletedDelegate = deleted ?? throw new ArgumentNullException(nameof(deleted));
            return this;
        }
    }
}
