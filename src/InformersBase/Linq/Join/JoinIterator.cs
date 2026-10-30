using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    /// <summary>
    ///     Common loop of Join and GroupJoin. It pulls both inputs in one loop (one side processed at a time), turns each input change into the
    ///     result keys it affects, and emits per result key by comparing the recomputed result with the one it last sent. It starts with one
    ///     reset once both inputs finished their first reset. Results are computed when the consumer pulls, so changes merge per result key.
    /// </summary>
    internal abstract class JoinIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResultKey, TResult> : IAsyncEnumerator<ResourceEvent<TResultKey, TResult>>
    {
        private readonly CancellationTokenSource _cts;
        private readonly IEqualityComparer<TResult> _resultComparer;
        private readonly Dictionary<TResultKey, TResult> _sent = new Dictionary<TResultKey, TResult>();
        private readonly HashQueue<TResultKey> _dirty = new HashQueue<TResultKey>();
        private readonly HashSet<TResultKey> _computed = new HashSet<TResultKey>();
        private Queue<ResourceEvent<TResultKey, TResult>> _reset;
        private bool _initialized;

        protected JoinIterator(
            IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
            Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
            IEqualityComparer<TResult> resultComparer,
            CancellationToken cancellationToken)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _resultComparer = resultComparer ?? EqualityComparer<TResult>.Default;
            Outer = JoinSide<TOuterKey, TOuter, TJoinKey>.Open(outer, outerKeySelector, _cts.Token);
            Inner = JoinSide<TInnerKey, TInner, TJoinKey>.Open(inner, innerKeySelector, _cts.Token);
        }

        protected JoinSide<TOuterKey, TOuter, TJoinKey> Outer { get; }
        protected JoinSide<TInnerKey, TInner, TJoinKey> Inner { get; }

        /// <summary>Result keys whose last sent result involves the given outer/inner key (so a change to it may remove or change them).</summary>
        protected IReadOnlyDictionary<TResultKey, TResult> Sent => _sent;

        public ResourceEvent<TResultKey, TResult> Current { get; private set; }

        /// <summary>Result keys affected by a change of an outer object.</summary>
        protected abstract IEnumerable<TResultKey> AffectedByOuter(TOuterKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys);

        /// <summary>Result keys affected by a change of an inner object.</summary>
        protected abstract IEnumerable<TResultKey> AffectedByInner(TInnerKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys);

        /// <summary>Every result key once both inputs are synced.</summary>
        protected abstract IEnumerable<TResultKey> AllResultKeys();

        /// <summary>Computes the current result for a key; false when there is none.</summary>
        protected abstract bool TryCompute(TResultKey key, out TResult result);

        protected virtual void OnSent(TResultKey key, bool exists)
        {
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            while (true)
            {
                _cts.Token.ThrowIfCancellationRequested();
                if (_reset != null)
                {
                    if (_reset.Count > 0)
                    {
                        Current = _reset.Dequeue();
                        return true;
                    }

                    _reset = null;
                }

                if (_initialized && _dirty.Count > 0)
                {
                    if (TryEmit(_dirty.Dequeue()))
                        return true;
                    continue;
                }

                if (!_initialized && Outer.Synced && Inner.Synced)
                {
                    EmitInitialReset();
                    continue;
                }

                if (Outer.Completed && Inner.Completed)
                    return false;

                var outerMove = Outer.Completed ? null : Outer.PendingMove;
                var innerMove = Inner.Completed ? null : Inner.PendingMove;
                if (outerMove != null && innerMove != null)
                    await Task.WhenAny(outerMove, innerMove).ConfigureAwait(false);
                else
                    await (outerMove ?? innerMove).ConfigureAwait(false);
                // one side at a time; if both completed, the other one is taken on the next round
                if (outerMove != null && outerMove.IsCompleted)
                {
                    foreach (var change in Outer.TakeCompleted())
                        MarkDirty(AffectedByOuter(change.Key, change.OldKeys, change.NewKeys), change.Computed);
                }
                else
                {
                    foreach (var change in Inner.TakeCompleted())
                        MarkDirty(AffectedByInner(change.Key, change.OldKeys, change.NewKeys), change.Computed);
                }
            }
        }

        private void MarkDirty(IEnumerable<TResultKey> keys, bool computed)
        {
            if (!_initialized)
                return; // the initial reset computes everything
            foreach (var key in keys)
            {
                _dirty.Enqueue(key);
                if (computed)
                    _computed.Add(key);
                else
                    _computed.Remove(key);
            }
        }

        private void EmitInitialReset()
        {
            _initialized = true;
            var items = new List<KeyValuePair<TResultKey, TResult>>();
            var seen = new HashSet<TResultKey>();
            foreach (var key in AllResultKeys())
            {
                if (!seen.Add(key) || !TryCompute(key, out var result))
                    continue;
                _sent[key] = result;
                OnSent(key, true);
                items.Add(new KeyValuePair<TResultKey, TResult>(key, result));
            }

            _reset = new Queue<ResourceEvent<TResultKey, TResult>>(ResetCollector<TResultKey, TResult>.ToResetBlock(items));
        }

        private bool TryEmit(TResultKey key)
        {
            var computed = _computed.Remove(key) ? EventTypeFlags.Computed : 0;
            var had = _sent.TryGetValue(key, out var previous);
            if (!TryCompute(key, out var result))
            {
                if (!had)
                    return false;
                _sent.Remove(key);
                OnSent(key, false);
                Current = new ResourceEvent<TResultKey, TResult>(EventTypeFlags.Delete | computed, key, previous);
                return true;
            }

            if (had && _resultComparer.Equals(previous, result))
                return false;
            _sent[key] = result;
            OnSent(key, true);
            Current = new ResourceEvent<TResultKey, TResult>((had ? EventTypeFlags.Modify : EventTypeFlags.Add) | computed, key, result);
            return true;
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await Outer.DisposeAsync().ConfigureAwait(false);
            await Inner.DisposeAsync().ConfigureAwait(false);
            _cts.Dispose();
        }
    }
}
