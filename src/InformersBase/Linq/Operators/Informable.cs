using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        public static IInformable<TKey, TResource> AsInformable<TKey, TResource>(this IAsyncEnumerable<ResourceEvent<TKey, TResource>> source)
        {
            return new InformableAdapter<TKey, TResource>(source);
        }
        

        private sealed class AnonymousInformable<TKey, TValue> : IInformable<TKey, TValue>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<ResourceEvent<TKey, TValue>>> _getEnumerator;

            public AnonymousInformable(Func<CancellationToken, IAsyncEnumerator<ResourceEvent<TKey, TValue>>> getEnumerator) => _getEnumerator = getEnumerator;

            public IAsyncEnumerator<ResourceEvent<TKey, TValue>> GetAsyncEnumerator(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested(); // NB: [LDM-2018-11-28] Equivalent to async iterator behavior.

                return _getEnumerator(cancellationToken);
            }
        }
        
        private static IAsyncEnumerable<T> CreateAsyncEnumerable<T>(Func<CancellationToken, IAsyncEnumerator<T>> getAsyncEnumerator) =>
            new AnonymousAsyncEnumerable<T>(getAsyncEnumerator);

        private sealed class AnonymousAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> _getEnumerator;

            public AnonymousAsyncEnumerable(Func<CancellationToken, IAsyncEnumerator<T>> getEnumerator) => _getEnumerator = getEnumerator;

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return _getEnumerator(cancellationToken);
            }
        }

        private static TResult SafeSelector<TSource, TResult>(TSource source, Func<TSource, TResult> selector)
        {
            return source == null ? default : selector(source);
        }
        public static IInformable<TKey, TResource> AsAsyncInformable<TKey, TResource>(this IAsyncEnumerable<ResourceEvent<TKey, TResource>> source)
        {
            return new InformableAdapter<TKey, TResource>(source);
        }
        

        internal static IInformable<TKey, TResource> TakeWhile<TKey, TResource>(this IInformable<TKey, TResource> source, Func<TResource, bool> predicate)
        {
            return source.Where(x => x.EventFlags.IsMeta() || predicate(x.Value)).AsInformable();
        }
        internal static IInformable<TKey, TResource> Take<TKey, TResource>(this IInformable<TKey, TResource> source, int count)
        {
            return ((IAsyncEnumerable<ResourceEvent<TKey, TResource>>) source).Take(count).AsInformable();
        }
        
        internal static IInformable<TKey, TResource> Skip<TKey, TResource>(this IInformable<TKey, TResource> source, int count)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TResource> SkipWhile<TKey, TResource>(this IInformable<TKey, TResource> source, Func<TResource, bool> predicate)
        {
            return source.Where(x => x.EventFlags.IsMeta() || predicate(x.Value)).AsInformable();
        }
        internal static IGroupedInformable<TGroupKey, TSourceKey, TSource> GroupBy<TSourceKey, TSource, TGroupKey>(this IInformable<TSourceKey, TSource> source,
            Func<TSource, TGroupKey> keySelector,
            IEqualityComparer<TGroupKey> comparer = default)
        {
            throw new NotImplementedException();
        }
        internal static IGroupedInformable<TGroupKey, TSourceKey, TResult> GroupBy<TSourceKey, TSource, TGroupKey, TResult>(this IInformable<TSourceKey, TSource> source,
            Func<TSource, TGroupKey> keySelector,
            Func<TSource, TResult> elementSelector,
            IEqualityComparer<TGroupKey> comparer = default)
        {
            throw new NotImplementedException();
        }
        internal static IGroupedInformable<TGroupKey, TSourceKey, TResult> GroupBy<TSourceKey, TSource, TGroupKey, TResult>(this IInformable<TSourceKey, TSource> source,
            Func<TSource, TGroupKey> keySelector,
            Func<TSource, TResult> elementSelector,
            Func<TGroupKey, IInformable<TSourceKey,TResult>, TResult> resultSelector,
            IEqualityComparer<TGroupKey> comparer = default)
        {
            throw new NotImplementedException();
        }
        internal static IGroupedInformable<TGroupKey, TSourceKey, TResult> GroupBy<TSourceKey, TSource, TGroupKey, TResult>(this IInformable<TSourceKey, TSource> source,
            Func<TSource, TGroupKey> keySelector,
            Func<TGroupKey, IInformable<TSourceKey,TSource>, TResult> resultSelector,
            IEqualityComparer<TGroupKey> comparer = default)
        {
            throw new NotImplementedException();
        }

        // skipping concat as you can't have duplicates in informable

        // skipping Zip - can't think under which scenario you would wanna do it
        
        internal static IInformable<TKey, TSource> Union<TKey, TSource>(this IInformable<TKey, TSource> source, IInformable<TKey, TSource> source2)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Intersect<TKey, TSource>(this IInformable<TKey, TSource> source, IInformable<TKey, TSource> source2, IEqualityComparer<TKey> comparer = default)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Except<TKey, TSource>(this IInformable<TKey, TSource> source, IInformable<TKey, TSource> source2, IEqualityComparer<TKey> comparer = default)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> First<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> FirstOrDefault<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Last<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Last<TKey, TSource>(this IInformable<TKey, TSource> source, Func<TSource, bool> predicate)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> LastOrDefault<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> LastOrDefault<TKey, TSource>(this IInformable<TKey, TSource> source, Func<TSource, bool> predicate)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Single<TKey, TSource>(this IInformable<TKey, TSource> source, Func<TSource, bool> predicate)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Single<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> SingleOrDefault<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> SingleOrDefault<TKey, TSource>(this IInformable<TKey, TSource> source, Func<TSource, bool> predicate)
        {
            throw new NotImplementedException();
        }
       
        internal static IInformable<TKey, TSource> Min<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Min<TKey, TSource, TResult>(this IInformable<TKey, TSource> source, Func<TSource, TResult> selector)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Max<TKey, TSource>(this IInformable<TKey, TSource> source)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TSource> Max<TKey, TSource, TResult>(this IInformable<TKey, TSource> source, Func<TSource, TResult> selector)
        {
            throw new NotImplementedException();
        }
        // is sum needed?

        internal static IInformable<TResultKey, TAccumulate> Aggregate<TKey, TSource, TAccumulate, TResultKey>(this IQueryable<TSource> source,
            TResultKey accumulateKey,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> func)
        {
            throw new NotImplementedException();

        }
        internal static IInformable<TKey, TResult> Aggregate<TKey, TSource, TAccumulate, TResultKey, TResult>(this IQueryable<TSource> source,
            TResultKey resultKey,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> func,
            Func<TAccumulate, TResult> selector)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TResource> SkipLast<TKey, TResource>(this IInformable<TKey, TResource> source, int count)
        {
            throw new NotImplementedException();
        }
        internal static IInformable<TKey, TResource> TakeLast<TKey, TResource>(this IInformable<TKey, TResource> source, int count)
        {
            throw new NotImplementedException();
        }
        // public static IInformable<TKey, TResource> Do<TKey, TResource>(this IInformable<TKey, TResource> source, Action<EventTypeFlags, TKey, TResource> action)
        // {
        //     throw new NotImplementedException();
        // }
        // skipping append / prepend
    }

    public interface IGroupedInformable<TGroupKey, TKey, TElement> : IInformable<TKey, TElement>
    {
        TGroupKey Key { get; }
    }
}