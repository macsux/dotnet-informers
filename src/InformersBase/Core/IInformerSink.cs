using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     What a producer writes to: the shared cache of one informer. Exactly one producer writes to a sink at a time, so its methods are
    ///     never called concurrently.
    /// </summary>
    /// <typeparam name="TResource">The type of resource</typeparam>
    public interface IInformerSink<in TResource>
    {
        /// <summary>
        ///     Replaces the whole content with a complete list (the initial list, or a relist). The first call marks the informer synced. A later
        ///     call is a relist: consumers receive computed Add/Modify/Delete events for the differences only, never a reset.
        /// </summary>
        void Replace(IEnumerable<TResource> items);

        /// <summary>Adds or updates one object.</summary>
        void Upsert(TResource item);

        /// <summary>Removes one object.</summary>
        void Delete(TResource item);
    }

    /// <summary>
    ///     Fills a sink (list, watch, relist and retry) until cancelled. Returning ends the informer normally; throwing ends it with a fatal
    ///     error that every consumer receives from its next <c>MoveNextAsync</c>.
    /// </summary>
    /// <param name="sink">The shared cache to write to</param>
    /// <param name="filter">The server filter consumers of this producer pushed down</param>
    /// <param name="cancellationToken">Cancelled when the last consumer leaves</param>
    public delegate Task InformerProducer<TResource>(IInformerSink<TResource> sink, ServerFilter filter, CancellationToken cancellationToken);
}
