using System.Collections.Generic;
using System.Threading;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Correlates two informables: one result per outer object, computed from the outer object and every inner object sharing a join key
        ///     with it, recomputed when either side changes. The result keeps the informable contract: it starts with a reset once both inputs
        ///     are synced, merges changes per outer key, and sends nothing when a recomputed result equals the last one sent (value equality, or
        ///     <paramref name="resultComparer" />).
        /// </summary>
        /// <remarks>
        ///     Over a source informer, matches are looked up through an index on its shared cache; over any other informable (a join, a
        ///     <c>Select</c>, a client-side <c>Where</c>) the join keeps its own index of references. A key selector that throws or returns null
        ///     matches nothing.
        /// </remarks>
        public static IInformable<TOuterKey, TResult> GroupJoin<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, TJoinKey> outerKeySelector,
            Func<TInner, TJoinKey> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            GroupJoin(outer, inner, Single(outerKeySelector), Single(innerKeySelector), resultSelector, resultComparer);

        /// <inheritdoc cref="GroupJoin{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,IEnumerable{TInner},TResult},IEqualityComparer{TResult})" />
        public static IInformable<TOuterKey, TResult> GroupJoin<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, TJoinKey> outerKeySelector,
            Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            GroupJoin(outer, inner, Single(outerKeySelector), innerKeySelector, resultSelector, resultComparer);

        /// <inheritdoc cref="GroupJoin{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,IEnumerable{TInner},TResult},IEqualityComparer{TResult})" />
        public static IInformable<TOuterKey, TResult> GroupJoin<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
            Func<TInner, TJoinKey> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
            IEqualityComparer<TResult> resultComparer = null) =>
            GroupJoin(outer, inner, outerKeySelector, Single(innerKeySelector), resultSelector, resultComparer);

        /// <summary>
        ///     <see cref="GroupJoin{TOuterKey,TOuter,TInnerKey,TInner,TJoinKey,TResult}(IInformable{TOuterKey,TOuter},IInformable{TInnerKey,TInner},Func{TOuter,TJoinKey},Func{TInner,TJoinKey},Func{TOuter,IEnumerable{TInner},TResult},IEqualityComparer{TResult})" />
        ///     with multi-valued keys: an object matches every object it shares at least one join key with (e.g. owner references, labels).
        /// </summary>
        public static IInformable<TOuterKey, TResult> GroupJoin<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(
            this IInformable<TOuterKey, TOuter> outer,
            IInformable<TInnerKey, TInner> inner,
            Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
            Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
            Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
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
            return Create<TOuterKey, TResult>(cancellationToken =>
                new GroupJoinIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, resultComparer, cancellationToken));
        }

        private static Func<T, IEnumerable<TJoinKey>> Single<T, TJoinKey>(Func<T, TJoinKey> selector)
        {
            if (selector == null)
                throw Error.ArgumentNull(nameof(selector));
            return x => new[] { selector(x) };
        }

        private sealed class GroupJoinIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TResult> : JoinIterator<TOuterKey, TOuter, TInnerKey, TInner, TJoinKey, TOuterKey, TResult>
        {
            private readonly Func<TOuter, IEnumerable<TInner>, TResult> _resultSelector;

            public GroupJoinIterator(
                IInformable<TOuterKey, TOuter> outer,
                IInformable<TInnerKey, TInner> inner,
                Func<TOuter, IEnumerable<TJoinKey>> outerKeySelector,
                Func<TInner, IEnumerable<TJoinKey>> innerKeySelector,
                Func<TOuter, IEnumerable<TInner>, TResult> resultSelector,
                IEqualityComparer<TResult> resultComparer,
                CancellationToken cancellationToken)
                : base(outer, inner, outerKeySelector, innerKeySelector, resultComparer, cancellationToken)
            {
                _resultSelector = resultSelector;
            }

            protected override IEnumerable<TOuterKey> AffectedByOuter(TOuterKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys) => new[] { key };

            protected override IEnumerable<TOuterKey> AffectedByInner(TInnerKey key, TJoinKey[] oldKeys, TJoinKey[] newKeys)
            {
                // old and new join keys, so a changed label or owner regroups the inner object correctly
                foreach (var joinKey in oldKeys.Concat(newKeys).Distinct())
                {
                    foreach (var match in Outer.Lookup(joinKey))
                        yield return match.Key;
                }
            }

            protected override IEnumerable<TOuterKey> AllResultKeys() => Outer.KnownKeys.ToList();

            protected override bool TryCompute(TOuterKey key, out TResult result)
            {
                if (!Outer.TryGet(key, out var outer))
                {
                    result = default;
                    return false;
                }

                var group = new List<TInner>();
                var seen = new HashSet<TInnerKey>();
                foreach (var joinKey in Outer.JoinKeysOf(outer))
                {
                    foreach (var match in Inner.Lookup(joinKey))
                    {
                        if (seen.Add(match.Key))
                            group.Add(match.Value);
                    }
                }

                result = _resultSelector(outer, group);
                return true;
            }
        }
    }
}
