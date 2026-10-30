using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     A shared informer: a level-triggered replica of a collection of <typeparamref name="TResource" /> that supports list/watch.
    ///     Enumerating it attaches a consumer to one shared producer (one list/watch connection per distinct server filter, however many
    ///     consumers are attached). Every consumer starts with a reset holding the current state, then receives changes merged per key.
    /// </summary>
    /// <remarks>
    ///     Objects are shared by reference between every consumer and must not be modified; clone an object before changing it.
    /// </remarks>
    /// <typeparam name="TKey">The type of key</typeparam>
    /// <typeparam name="TResource">The type of resource</typeparam>
    public interface IInformer<TKey, TResource> : IInformable<TKey, TResource>
    {
        /// <summary>Lists the current objects once, without attaching a consumer.</summary>
        IAsyncEnumerable<TResource> List(CancellationToken cancellationToken = default);

        /// <summary>The informer itself, kept for compatibility: an informer is an informable.</summary>
        IInformable<TKey, TResource> ListWatch();

        /// <summary>
        ///     Whether the producer serving this informer (for its server filter) is running and has applied its first complete list.
        /// </summary>
        bool IsSynced { get; }

        /// <summary>
        ///     Completes once the producer serving this informer has applied its first complete list. Producers only run while a consumer is
        ///     attached, so this waits for a consumer (e.g. an <c>Into</c> or <c>RunAsync</c>) to attach and the list to arrive. Throws
        ///     <see cref="InformerException" /> if the producer fails.
        /// </summary>
        Task WaitForSyncAsync(CancellationToken cancellationToken = default);

        /// <summary>
        ///     Filters the informer. Terms of the predicate the source can translate are pushed down to the server subscription and become part
        ///     of the sharing key; the rest is evaluated client-side. Objects that stop matching arrive as deletes, objects that start matching
        ///     as adds.
        /// </summary>
        IInformer<TKey, TResource> Where(Expression<Func<TResource, bool>> predicate);
    }

    /// <summary>
    ///     An informer whose subscriptions take explicit options of type <typeparamref name="TOptions" />.
    /// </summary>
    public interface IInformer<TKey, TResource, in TOptions>
    {
        /// <summary>Lists the current objects matching <paramref name="options" /> once.</summary>
        IAsyncEnumerable<TResource> List(TOptions options, CancellationToken cancellationToken = default);

        /// <summary>The shared informer for the given options.</summary>
        IInformer<TKey, TResource> ListWatch(TOptions options);
    }
}
