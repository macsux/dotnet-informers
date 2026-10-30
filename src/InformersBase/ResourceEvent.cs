using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace Steeltoe.Informers.InformersBase
{
    public struct ResourceEvent
    {
        public static ResourceEvent<TKey, TResource> Create<TKey, TResource>(EventTypeFlags eventFlags, TKey key, TResource value = default) =>
            new ResourceEvent<TKey, TResource>(eventFlags, key, value);
    }
    /// <summary>
    /// </summary>
    /// <typeparam name="TResource"></typeparam>
    [DebuggerStepThrough]
    public struct ResourceEvent<TKey, TResource>
    {
        

        public static ResourceEvent<TKey, TResource> EmptyReset { get; } = new ResourceEvent<TKey, TResource>(EventTypeFlags.ResetEmpty);
        public static ResourceEvent<TKey, TResource> ResetEnd { get; } = new ResourceEvent<TKey, TResource>(EventTypeFlags.ResetEnd);

        public ResourceEvent(EventTypeFlags eventFlags, TKey key = default, TResource value = default)
        {
            if (eventFlags.HasFlag(EventTypeFlags.ResetEmpty))
            {
                eventFlags |= EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd;
            }

            if (eventFlags.HasFlag(EventTypeFlags.ResetEnd) || eventFlags.HasFlag(EventTypeFlags.ResetStart))
            {
                eventFlags |= EventTypeFlags.Reset;
            }

            if (eventFlags.HasFlag(EventTypeFlags.Reset) || eventFlags.HasFlag(EventTypeFlags.Sync))
            {
                eventFlags |= EventTypeFlags.Current;
            }

            Key = key;
            Value = value;
            EventFlags = eventFlags;
        }

        public EventTypeFlags EventFlags { get; }
        public TKey Key { get; set; }
        public TResource Value { get; }

        public override string ToString()
        {
            var includePrefix = Value != null;

            var sb = new StringBuilder();
            sb.AppendLine();
            sb.Append("   ");
            sb.Append(EventFlags);
            sb.Append($": [Key={Key} ");
            
            if (Value != null)
            {
                if (includePrefix)
                {
                    sb.Append(nameof(Value));
                    sb.Append("{ ");
                }

                sb.Append(Value);
                if (includePrefix)
                {
                    sb.Append("} ");
                }
            }

            sb.Append("]");
            return sb.ToString();
        }
    }

    public static class ResourceEventExtensions
    {
        
        public static ResourceEvent<TKey, TResource> With<TKey, TResource>(this ResourceEvent<TKey, TResource> obj, EventTypeFlags typeFlags = default, TKey key = default, TResource value = default)
        {
            if (EqualityComparer<EventTypeFlags>.Default.Equals(typeFlags, default))
                typeFlags = obj.EventFlags;
            if (EqualityComparer<TKey>.Default.Equals(key, default))
                key = obj.Key;
            if (EqualityComparer<TResource>.Default.Equals(value, default))
                value = obj.Value;

            
            return new ResourceEvent<TKey, TResource>(typeFlags, key, value);
        }



        public static ResourceEvent<TResource, TResource> ToResourceEvent<TResource>(this TResource obj, EventTypeFlags typeFlags)
        {
            return new ResourceEvent<TResource, TResource>(typeFlags, obj, obj);

        }
        public static ResourceEvent<TKey, TResource> ToResourceEvent<TKey, TResource>(this TResource obj, EventTypeFlags typeFlags, TKey key)
        {
            return new ResourceEvent<TKey, TResource>(typeFlags, key, obj);
        }

        public static IEnumerable<ResourceEvent<TKey, TResource>> ToReset<TKey, TResource>(this IEnumerable<ResourceEvent<TKey, TResource>> source, bool emitEmpty = false)
        {
            return ToReset(source, x => x.Key,  x => x.Value, emitEmpty);
        }

        /// <summary>
        ///     Converts a list of objects to a resource reset list event block. Every item is of type <see cref="EventTypeFlags.Reset" />,
        ///     with first and last elements also having <see cref="EventTypeFlags.ResetStart" /> and <see cref="EventTypeFlags.ResetEnd" />
        ///     set respectively. If <paramref name="source" /> is empty and <paramref name="emitEmpty" /> is set,
        /// </summary>
        /// <param name="source">The source enumerable</param>
        /// <param name="emitEmpty">
        ///     If <see langword="true" /> the resulting <see cref="IEnumerable{T}" /> will contain a single
        ///     <see cref="ResourceEvent{TResource}" /> with no object value and <see cref="EventTypeFlags.ResetEmpty" /> flag set
        /// </param>
        /// <typeparam name="TResource">The type of resource</typeparam>
        /// <returns>The resulting enumerable of reset events</returns>

        public static IEnumerable<ResourceEvent<TKey, TResource>> ToReset<TKey, TResource>(this IEnumerable<TResource> source, Func<TResource, TKey> keySelector, bool emitEmpty = false)
        {
            return ToReset(source, keySelector, x => x, emitEmpty);
        }
        public static IEnumerable<ResourceEvent<TKey, TResource>> ToReset<TKey, TResource>(this IDictionary<TKey, TResource> source,  bool emitEmpty = false)
        {
            return ToReset(source, pair => pair.Key, x => x.Value, emitEmpty);
        }

        public static IEnumerable<ResourceEvent<TKey, TResource>> ToReset<TSource, TKey, TResource>(
            this IEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Func<TSource, TResource> valueSelector,
            bool emitEmpty = false)
        {
            using var enumerator = source.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                if (emitEmpty)
                    yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.ResetEmpty, default);
                yield break;
            }

            var current = enumerator.Current;
            var flags = EventTypeFlags.ResetStart;
            while (enumerator.MoveNext())
            {
                yield return valueSelector(current).ToResourceEvent(flags, keySelector(current));
                flags = EventTypeFlags.Reset;
                current = enumerator.Current;
            }

            yield return valueSelector(current).ToResourceEvent(flags | EventTypeFlags.ResetEnd, keySelector(current));
        }

        public static async IAsyncEnumerable<ResourceEvent<TKey, TResource>> ToReset<TSource, TKey, TResource>(this IAsyncEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Func<TSource, TResource> valueSelector,
            bool emitEmpty = false,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var i = 0;
            var enumerator = source.GetAsyncEnumerator(cancellationToken);
            
            if (!await enumerator.MoveNextAsync())
            {
                if (emitEmpty)
                {
                    yield return new ResourceEvent<TKey, TResource>(EventTypeFlags.ResetEmpty, default);
                }
                yield break;
            }

            var current = enumerator.Current;
            var value = valueSelector(current);
            var key = keySelector(current);
            while (await enumerator.MoveNextAsync())
            {
                if (i == 0)
                {
                    yield return value.ToResourceEvent(EventTypeFlags.ResetStart, key);
                }
                else
                {
                    yield return value.ToResourceEvent(EventTypeFlags.Reset, key);
                }
                current = enumerator.Current;
                value = valueSelector(current);
                key = keySelector(current);
                i++;
            }

            if (i == 0)
            {
                yield return value.ToResourceEvent(EventTypeFlags.ResetStart | EventTypeFlags.ResetEnd, key);
            }
            else
            {
                yield return value.ToResourceEvent(EventTypeFlags.ResetEnd, key);
            }

            await enumerator.DisposeAsync();
        }

        
    }
}
