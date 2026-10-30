using System;
using System.Collections.Generic;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     How a <see cref="SharedInformer{TKey,TResource}" /> keys, transforms, compares and filters its objects. Options are part of the
    ///     informer's identity: two informers with different options never share a producer.
    /// </summary>
    public sealed class SharedInformerOptions<TKey, TResource>
    {
        /// <summary>Selects the key of an object. Required.</summary>
        public Func<TResource, TKey> KeySelector { get; set; }

        /// <summary>Compares keys. Defaults to <see cref="EqualityComparer{T}.Default" />.</summary>
        public IEqualityComparer<TKey> KeyComparer { get; set; }

        /// <summary>
        ///     Optional no-op check: an update whose content this comparer considers equal to what a consumer last received is not delivered
        ///     to it. Without one, every applied update counts as a change (the cache revision decides).
        /// </summary>
        public IEqualityComparer<TResource> ChangeComparer { get; set; }

        /// <summary>Translates <c>Where</c> terms into server filters. Without one, every predicate is evaluated client-side.</summary>
        public IFilterTranslator<TResource> FilterTranslator { get; set; }

        /// <summary>
        ///     Runs in the producer on every object before it's cached and shared, e.g. to drop fields no consumer reads. It's the one
        ///     place where mutating an object is allowed; it must return the object to cache.
        /// </summary>
        public Func<TResource, TResource> Transform { get; set; }

        /// <summary>Named indexes maintained on the shared cache.</summary>
        public IDictionary<string, Func<TResource, IEnumerable<string>>> Indexes { get; } = new Dictionary<string, Func<TResource, IEnumerable<string>>>();

        /// <summary>
        ///     Opt-in mutation detector for tests and debug builds: a function producing a fingerprint (e.g. serialized JSON) of an object. The
        ///     cache fingerprints every object it stores and fails the informer when one changes after it was handed out.
        /// </summary>
        public Func<TResource, string> MutationFingerprint { get; set; }

        /// <summary>A name used in metrics and logs.</summary>
        public string Name { get; set; }
    }
}
