using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;

namespace Steeltoe.Informers.InformersBase
{
    public static class InformerExtensions
    {
        /// <summary>
        ///     Removes an item from the dictionary
        /// </summary>
        /// <param name="source">The source dictionary</param>
        /// <param name="key">The key for which item should be removed</param>
        /// <param name="result">The value of the object that was removed, or <see langword="null" /> if value was not present in dictionary</param>
        /// <typeparam name="TKey">The type of key</typeparam>
        /// <typeparam name="TValue">The type of value</typeparam>
        /// <returns><see langword="true" /> if the object was removed from dictionry, or <see langword="false" /> if the specific key was not present in dictionary</returns>
        internal static bool Remove<TKey, TValue>(this IDictionary<TKey, TValue> source, TKey key, out TValue result)
        {
            result = default;
            if (!source.TryGetValue(key, out result))
            {
                return false;
            }

            source.Remove(key);
            return true;
        }


        /// <summary>
        ///     Creates a <see cref="HashSet{T}" /> for <see cref="IEnumerable{T}" />
        /// </summary>
        /// <param name="source">The source enumerable</param>
        /// <typeparam name="T">The type of elements</typeparam>
        /// <returns>The produced hashset</returns>
        internal static HashSet<T> ToHashSet<T>(this IEnumerable<T> source)
        {
            return source.ToHashSet(null);
        }

        /// <summary>
        ///     Creates a <see cref="HashSet{T}" /> for <see cref="IEnumerable{T}" />
        /// </summary>
        /// <param name="source">The source enumerable</param>
        /// <param name="comparer">The comparer to use</param>
        /// <typeparam name="T">The type of elements</typeparam>
        /// <returns>The produced hashset</returns>
        internal static HashSet<T> ToHashSet<T>(
            this IEnumerable<T> source,
            IEqualityComparer<T> comparer)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return new HashSet<T>(source, comparer);
        }


#pragma warning disable 1998
        internal static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IEnumerable<T> source)
#pragma warning restore 1998
        {
            foreach (var item in source)
            {
                yield return item;
            }
        }

        /// <summary>
        ///     Returns the shared informer of an <see cref="IInformer{TKey,TResource,TOptions}" /> for one set of options
        /// </summary>
        public static IInformer<TKey, TResource> WithOptions<TKey, TResource, TOptions>(this IInformer<TKey, TResource, TOptions> optionedInformer, TOptions options) =>
            optionedInformer.ListWatch(options);
    }
}
