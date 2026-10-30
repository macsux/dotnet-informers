using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IEnumerable<ResourceEvent<TKey, TValue>> updates)
        {
            return Create(new Dictionary<TKey, TValue>(), updates);
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IAsyncEnumerable<ResourceEvent<TKey, TValue>> updates)
        {
            return Create(new Dictionary<TKey, TValue>(), updates);
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IDictionary<TKey, TValue> initial)
        {
            return Create(initial, Enumerable.Empty<ResourceEvent<TKey,TValue>>());
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IEnumerable<TValue> initial, Func<TValue, TKey> keySelector)
        {
            return Create(initial, keySelector, Enumerable.Empty<ResourceEvent<TKey,TValue>>());
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IEnumerable<TValue> initial, Func<TValue, TKey> keySelector, IEnumerable<ResourceEvent<TKey, TValue>> updates)
        {
            return Create(initial.ToDictionary(keySelector, x => x), updates);
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IEnumerable<TValue> initial, Func<TValue, TKey> keySelector, IAsyncEnumerable<ResourceEvent<TKey, TValue>> updates)
        {
            return Create(initial.ToDictionary(keySelector, x => x), updates);
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IDictionary<TKey, TValue> initial, IAsyncEnumerable<ResourceEvent<TKey,TValue>> updates)
        {
            async IAsyncEnumerator<ResourceEvent<TKey, TValue>> GetEnumerator()
            {
                foreach (var item in initial.ToReset(true))
                    yield return item;
                await foreach (var item in updates)
                    yield return item;
            }

            return Create(cts => GetEnumerator());
        }
        public static IInformable<TKey, TValue> Create<TKey, TValue>(IDictionary<TKey, TValue> initial, IEnumerable<ResourceEvent<TKey,TValue>> updates)
        {
            async IAsyncEnumerator<ResourceEvent<TKey, TValue>> GetEnumerator()
            {
                foreach (var item in initial.ToReset(true))
                    yield return item;
                foreach (var item in updates)
                    yield return item;
            }

            return Create(cts => GetEnumerator());
        }

        /// <summary>
        /// Creates a new enumerable using the specified delegates implementing the members of <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        /// <typeparam name="TElement">The type of the elements returned by the enumerable sequence.</typeparam>
        /// <param name="getAsyncEnumerator">The delegate implementing the <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> method.</param>
        /// <returns>A new enumerable instance.</returns>
        public static IInformable<TKey, TElement> Create<TKey, TElement>(Func<CancellationToken, IAsyncEnumerator<ResourceEvent<TKey, TElement>>> getAsyncEnumerator)
        {
            if (getAsyncEnumerator == null)
                throw Error.ArgumentNull(nameof(getAsyncEnumerator));

            return new AnonymousInformable<TKey, TElement>(getAsyncEnumerator);
        }
    }
}