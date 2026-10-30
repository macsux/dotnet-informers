using System.Collections.Generic;
using System.Threading;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Correlates two informables: one result per matching (outer, inner) pair, keyed by the pair of their keys, recomputed when either
        ///     object changes. Same contract as
        ///     <see cref="GroupJoin{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,IEnumerable{TInner},TResult},IEqualityComparer{TResult})" />.
        /// </summary>
        public static IInformable<(TOuterKey, TInnerKey), TResult> Join<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, TJoinKey> outerKeySelector,
            Func<TInner, TJoinKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            Join(outer, inner, Single(outerKeySelector), Single(innerKeySelector), resultSelector, resultComparer);

        /// <inheritdoc cref="Join{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,TInner,TResult},IEqualityComparer{TResult})" />
        public static IInformable<(TOuterKey, TInnerKey), TResult> Join<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, TJoinKey> outerKeySelector,
            Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            Join(outer, inner, Single(outerKeySelector), innerKeySelector, resultSelector, resultComparer);

        /// <inheritdoc cref="Join{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,TInner,TResult},IEqualityComparer{TResult})" />
        public static IInformable<(TOuterKey, TInnerKey), TResult> Join<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
            Func<TInner, TJoinKey> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            Join(outer, inner, outerKeySelector, Single(innerKeySelector), resultSelector, resultComparer);

        /// <summary>
        ///     <see cref="Join{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,TInner,TResult},IEqualityComparer{TResult})" />
        ///     with multi-valued keys: a pair matches when the two objects share at least one join key.
        /// </summary>
        public static IInformable<(TOuterKey, TInnerKey), TResult> Join<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
            Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
            Func<TOuter, TInner, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null)
        {
            if (outer == null)
                throw Error.ArgumentNull(nameof(outer));
            if (inner == null)
                throw Error.ArgumentNull(nameof(inner));
            if (outerKeySelector == null)
                throw Error.ArgumentNull(nameof(outerKeySelector));
            if (innerKeySelector == null)
                throw Error.ArgumentNull(nameof(innerKeySelector));
            if (resultSelector == null)
                throw Error.ArgumentNull(nameof(resultSelector));
            return Create<(TOuterKey, TInnerKey), TResult>(cancellationToken =>
                new JoinPairIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, resultComparer, cancellationToken));
        }

        private sealed class JoinPairIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult> : JoinIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, (TOuterKey, TInnerKey), TResult>
        {
            private readonly Func<TOuter, TInner, TResult> _resultSelector;
            private readonly Dictionary<TOuterKey, HashSet<TInnerKey>> _sentByOuter = new Dictionary<TOuterKey, HashSet<TInnerKey>>();
            private readonly Dictionary<TInnerKey, HashSet<TOuterKey>> _sentByInner = new Dictionary<TInnerKey, HashSet<TOuterKey>>();

            public JoinPairIterator(
                IInformable<TOuterKey, TOuter> outer,
                IInformable<TInnerKey, TInner> inner,
                Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
                Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
                Func<TOuter, TInner, TResult> resultSelector,
                IEqualityComparer<TResult> resultComparer,
                CancellationToken cancellationToken)
                : base(outer, inner, outerKeySelector, innerKeySelector, resultComparer, cancellationToken)
            {
                _resultSelector = resultSelector;
            }

            protected override IEnumerable<(TOuterKey, TInnerKey)> AffectedByOuter(TOuterKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys)
            {
                var affected = new List<(TOuterKey, TInnerKey)>();
                if (_sentByOuter.TryGetValue(key, out var sent))
                    affected.AddRange(sent.Select(inner => (key, inner)));
                foreach (var joinKey in newKeys)
                    affected.AddRange(Inner.Lookup(joinKey).Select(match => (key, match.Key)));
                return affected;
            }

            protected override IEnumerable<(TOuterKey, TInnerKey)> AffectedByInner(TInnerKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys)
            {
                var affected = new List<(TOuterKey, TInnerKey)>();
                if (_sentByInner.TryGetValue(key, out var sent))
                    affected.AddRange(sent.Select(outer => (outer, key)));
                foreach (var joinKey in newKeys)
                    affected.AddRange(Outer.Lookup(joinKey).Select(match => (match.Key, key)));
                return affected;
            }

            protected override IEnumerable<(TOuterKey, TInnerKey)> AllResultKeys()
            {
                var keys = new List<(TOuterKey, TInnerKey)>();
                foreach (var outerKey in Outer.KnownKeys.ToList())
                {
                    if (!Outer.TryGet(outerKey, out var outer))
                        continue;
                    foreach (var joinKey in Outer.JoinKeysOf(outer))
                        keys.AddRange(Inner.Lookup(joinKey).Select(match => (outerKey, match.Key)));
                }

                return keys;
            }

            protected override bool TryCompute((TOuterKey, TInnerKey) key, out TResult result)
            {
                result = default;
                if (!Outer.TryGet(key.Item1, out var outer) || !Inner.TryGet(key.Item2, out var inner))
                    return false;
                var innerKeys = new HashSet<TJoinKey>(Inner.JoinKeysOf(inner));
                if (!Outer.JoinKeysOf(outer).Any(innerKeys.Contains))
                    return false;
                result = _resultSelector(outer, inner);
                return true;
            }

            protected override void OnSent((TOuterKey, TInnerKey) key, bool exists)
            {
                if (exists)
                {
                    if (!_sentByOuter.TryGetValue(key.Item1, out var byOuter))
                        _sentByOuter[key.Item1] = byOuter = new HashSet<TInnerKey>();
                    byOuter.Add(key.Item2);
                    if (!_sentByInner.TryGetValue(key.Item2, out var byInner))
                        _sentByInner[key.Item2] = byInner = new HashSet<TOuterKey>();
                    byInner.Add(key.Item1);
                    return;
                }

                if (_sentByOuter.TryGetValue(key.Item1, out var outerSet) && outerSet.Remove(key.Item2) && outerSet.Count == 0)
                    _sentByOuter.Remove(key.Item1);
                if (_sentByInner.TryGetValue(key.Item2, out var innerSet) && innerSet.Remove(key.Item1) && innerSet.Count == 0)
                    _sentByInner.Remove(key.Item2);
            }
        }
    }
}
