using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
public static partial class Informable
    {
        public static IInformable<TKey, TResource> WithResets<TKey, TResource>(this IInformable<TKey, TResource> source, Func<List<ResourceEvent<TKey, TResource>>, IEnumerable<ResourceEvent<TKey, TResource>>> action)
        {
            return source.WithResets(action, e => e).AsInformable();
        }

        public static IAsyncEnumerable<TOut> WithResets<TKey, TResource, TOut>(
            this IInformable<TKey, TResource> source,
            Func<List<ResourceEvent<TKey, TResource>>, IEnumerable<ResourceEvent<TKey, TResource>>> resetSelector,
            Func<ResourceEvent<TKey, TResource>, TOut> itemSelector)
        {
            var resetBuffer = new List<ResourceEvent<TKey, TResource>>();

            
            async IAsyncEnumerator<TOut> SourceEnumerator(CancellationToken token)
            {
                IEnumerable<TOut> FlushBuffer()
                {
                    if (!resetBuffer.Any())
                    {
                        yield break;
                    }

                    foreach (var item in resetSelector(resetBuffer))
                    {
                        if(token.IsCancellationRequested)
                            yield break;
                        yield return itemSelector(item);
                    }
                    resetBuffer.Clear();
                }
                await foreach (var notification in source.WithCancellation(token))
                {
                    var isExplicitResetEnd = notification.EventFlags.HasFlag(EventTypeFlags.ResetEnd);

                    if (notification.EventFlags.HasFlag(EventTypeFlags.Reset))
                    {
                        resetBuffer.Add(notification);
                        if (!notification.EventFlags.HasFlag(EventTypeFlags.ResetEnd)) // continue buffering till we reach the end of list window
                        {
                            continue;
                        }
                    }

                    var isImplicitResetEnd = !notification.EventFlags.HasFlag(EventTypeFlags.Reset) && resetBuffer.Count > 0;
                    if (isExplicitResetEnd || isImplicitResetEnd)
                    {
                        foreach (var bufferItem in FlushBuffer())
                        {
                            yield return bufferItem;
                        }

                        if (isExplicitResetEnd)
                        {
                            // item was already flushed as part of reset buffer
                            continue;
                        }
                    }

                    yield return itemSelector(notification);

                }
            }
            return CreateAsyncEnumerable(SourceEnumerator);
        }

        internal class ResetExtractor<TKey, TResource>
        {
            List<ResourceEvent<TKey, TResource>> _resetBuffer = new List<ResourceEvent<TKey, TResource>>();
            public bool ApplyEvent(ResourceEvent<TKey, TResource> notification, out List<ResourceEvent<TKey, TResource>> reset)
            {
                
                reset = null;
                var isReset = notification.EventFlags.HasFlag(EventTypeFlags.Reset);
                var isExplicitResetEnd = notification.EventFlags.HasFlag(EventTypeFlags.ResetEnd);
                if (isReset)
                {
                    if (!IsResetting || notification.EventFlags.HasFlag(EventTypeFlags.ResetStart)) // start of new reset block
                    {
                        _resetBuffer.Clear();
                    }

                    IsResetting = true;
                    _resetBuffer.Add(notification);
                    if (!isExplicitResetEnd) // continue buffering till we reach the end of list window
                    {
                        return false;
                    }
                }

                var isImplicitResetEnd = !notification.EventFlags.HasFlag(EventTypeFlags.Reset) && _resetBuffer.Count > 0;
                if (isExplicitResetEnd || isImplicitResetEnd)
                {
                    reset =  _resetBuffer;
                    IsResetting = false;
                    return true;
                }

                return false;
            }

            public bool IsResetting { get; private set; }

            public async Task<List<ResourceEvent<TKey, TResource>>> Extract(IAsyncEnumerator<ResourceEvent<TKey, TResource>> source, CancellationToken cancellationToken = default)
            {
                while (!cancellationToken.IsCancellationRequested && await source.MoveNextAsync())
                {
                    if (ApplyEvent(source.Current, out var resetBuffer))
                        return resetBuffer;
                }
                return new List<ResourceEvent<TKey, TResource>>();
            }
        }
        
    }
}