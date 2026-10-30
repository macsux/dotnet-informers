using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Remembers creates and deletes a controller issued per owner until the informer observes them, so a reconcile that runs before the
    ///     watch events arrive doesn't act on stale state (e.g. create the same pods again). Like client-go's <c>ControllerExpectations</c>.
    /// </summary>
    /// <remarks>
    ///     Observations must happen in the pipeline that triggers the reconcile, before the join or handler sees the change: wrap the inner
    ///     informable with <see cref="Observe{TKey,TResource}" />. Merging can swallow an add followed by a delete before anyone pulls;
    ///     the timeout covers that case.
    /// </remarks>
    public sealed class Expectations
    {
        private readonly TimeSpan _timeout;
        private readonly Func<DateTime> _clock;
        private readonly ConcurrentDictionary<string, Expectation> _expectations = new ConcurrentDictionary<string, Expectation>();

        public Expectations(TimeSpan timeout) : this(timeout, () => DateTime.UtcNow)
        {
        }

        internal Expectations(TimeSpan timeout, Func<DateTime> clock)
        {
            _timeout = timeout;
            _clock = clock;
        }

        private sealed class Expectation
        {
            public int Adds;
            public int Deletes;
            public DateTime SetAt;
        }

        /// <summary>Expect <paramref name="count" /> creates of objects owned by <paramref name="ownerKey" />.</summary>
        public void ExpectCreations(string ownerKey, int count) => Raise(ownerKey, count, 0);

        /// <summary>Expect <paramref name="count" /> deletes of objects owned by <paramref name="ownerKey" />.</summary>
        public void ExpectDeletions(string ownerKey, int count) => Raise(ownerKey, 0, count);

        /// <summary>A create failed and will never be observed: lowers the expectation.</summary>
        public void CreationFailed(string ownerKey) => Lower(ownerKey, 1, 0);

        /// <summary>A delete failed (or the object was already gone) and will never be observed: lowers the expectation.</summary>
        public void DeletionFailed(string ownerKey) => Lower(ownerKey, 0, 1);

        /// <summary>The informer observed a create of an object owned by <paramref name="ownerKey" />.</summary>
        public void CreationObserved(string ownerKey) => Lower(ownerKey, 1, 0);

        /// <summary>The informer observed a delete of an object owned by <paramref name="ownerKey" />.</summary>
        public void DeletionObserved(string ownerKey) => Lower(ownerKey, 0, 1);

        /// <summary>
        ///     True when every expected create and delete for the owner was observed, nothing was expected, or the expectation timed out.
        /// </summary>
        public bool IsSatisfied(string ownerKey)
        {
            if (!_expectations.TryGetValue(ownerKey, out var expectation))
                return true;
            lock (expectation)
                return (expectation.Adds <= 0 && expectation.Deletes <= 0) || _clock() - expectation.SetAt >= _timeout;
        }

        /// <summary>Time left until an unsatisfied expectation times out (zero when satisfied), e.g. for <c>ReconcileResult.RequeueAfter</c>.</summary>
        public TimeSpan RemainingTime(string ownerKey)
        {
            if (!_expectations.TryGetValue(ownerKey, out var expectation))
                return TimeSpan.Zero;
            lock (expectation)
            {
                if (expectation.Adds <= 0 && expectation.Deletes <= 0)
                    return TimeSpan.Zero;
                var left = _timeout - (_clock() - expectation.SetAt);
                return left > TimeSpan.Zero ? left : TimeSpan.Zero;
            }
        }

        /// <summary>Forgets the owner (e.g. it was deleted).</summary>
        public void Delete(string ownerKey) => _expectations.TryRemove(ownerKey, out _);

        /// <summary>
        ///     Passes <paramref name="source" /> through, counting adds and deletes per owner key (from <paramref name="ownerKeys" />, e.g.
        ///     <c>pod =&gt; pod.OwnerKeys("AppSet")</c>) before anything downstream sees them. Objects in the initial reset count as adds.
        /// </summary>
        public IInformable<TKey, TResource> Observe<TKey, TResource>(IInformable<TKey, TResource> source, Func<TResource, IEnumerable<string>> ownerKeys)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (ownerKeys == null)
                throw new ArgumentNullException(nameof(ownerKeys));
            return source.Do(e =>
            {
                var flags = e.EventFlags;
                if (flags.HasFlag(EventTypeFlags.ResetEmpty) || e.Value == null)
                    return;
                IEnumerable<string> owners;
                try
                {
                    owners = ownerKeys(e.Value) ?? Enumerable.Empty<string>();
                }
                catch (Exception)
                {
                    return;
                }

                foreach (var owner in owners)
                {
                    if (flags.HasFlag(EventTypeFlags.Delete))
                        DeletionObserved(owner);
                    else if (flags.HasFlag(EventTypeFlags.Add) || flags.HasFlag(EventTypeFlags.Reset))
                        CreationObserved(owner);
                }
            });
        }

        private void Raise(string ownerKey, int adds, int deletes)
        {
            if (adds < 0 || deletes < 0)
                throw new ArgumentOutOfRangeException(nameof(adds));
            var expectation = _expectations.GetOrAdd(ownerKey, _ => new Expectation());
            lock (expectation)
            {
                if (expectation.Adds <= 0 && expectation.Deletes <= 0 || _clock() - expectation.SetAt >= _timeout)
                {
                    expectation.Adds = 0;
                    expectation.Deletes = 0;
                }

                expectation.Adds += adds;
                expectation.Deletes += deletes;
                expectation.SetAt = _clock();
            }
        }

        private void Lower(string ownerKey, int adds, int deletes)
        {
            if (!_expectations.TryGetValue(ownerKey, out var expectation))
                return;
            lock (expectation)
            {
                expectation.Adds -= adds;
                expectation.Deletes -= deletes;
            }
        }
    }
}
