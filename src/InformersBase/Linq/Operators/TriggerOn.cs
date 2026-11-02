using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Steeltoe.Informers.InformersBase;

namespace System.Linq
{
    public static partial class Informable
    {
        /// <summary>
        ///     Marks a modification <see cref="EventTypeFlags.Quiet" /> when none of the selected values changed since the last version this
        ///     consumer saw of that key, e.g. <c>TriggerOn(x =&gt; x.Spec, x =&gt; x.Metadata.Labels)</c> to ignore status-only updates such as the
        ///     controller's own status writes. A quiet change still carries the latest object: joins, caches and retries stay current, only
        ///     <c>RunAsync</c> doesn't start a reconcile for it. Adds, deletes, resets and syncs always trigger. Values compare structurally (see
        ///     <see cref="Trigger.On{T}(Expression{Func{T, object}}[])" />).
        /// </summary>
        /// <remarks>
        ///     Unlike <c>Where</c>, it never removes an object from the set. Apply it to a source before a join (the result triggers if any
        ///     contributing change does) or after the join on the combined value.
        /// </remarks>
        public static IInformable<TKey, TResource> TriggerOn<TKey, TResource>(this IInformable<TKey, TResource> source, params Expression<Func<TResource, object>>[] selectors) =>
            source.TriggerOn(Trigger.On(selectors));

        /// <summary>
        ///     <see cref="TriggerOn{TKey,TResource}(IInformable{TKey,TResource},Expression{Func{TResource,object}}[])" /> with a preset (e.g.
        ///     <c>Trigger.Generation</c>) and further selected values: a change triggers if either does.
        /// </summary>
        public static IInformable<TKey, TResource> TriggerOn<TKey, TResource>(this IInformable<TKey, TResource> source, ITrigger<TResource> trigger, params Expression<Func<TResource, object>>[] selectors) =>
            source.TriggerOn(selectors == null || selectors.Length == 0 ? trigger : Trigger.Combine(trigger, Trigger.On(selectors)));

        /// <summary>Marks a modification <see cref="EventTypeFlags.Quiet" /> unless <paramref name="trigger" /> reports a change.</summary>
        public static IInformable<TKey, TResource> TriggerOn<TKey, TResource>(this IInformable<TKey, TResource> source, ITrigger<TResource> trigger)
        {
            if (source == null)
                throw Error.ArgumentNull(nameof(source));
            if (trigger == null)
                throw Error.ArgumentNull(nameof(trigger));
            return Create<TKey, TResource>(cancellationToken => TriggerOnIterator(source, trigger, cancellationToken));
        }

        /// <summary>
        ///     Marks a modification <see cref="EventTypeFlags.Quiet" /> unless <paramref name="changed" /> returns true for the previous and current
        ///     object; for rules a projection can't express. It keeps the whole previous object per key.
        /// </summary>
        public static IInformable<TKey, TResource> TriggerWhen<TKey, TResource>(this IInformable<TKey, TResource> source, Func<TResource, TResource, bool> changed) =>
            source.TriggerOn(Trigger.When(changed));

        private static async IAsyncEnumerator<ResourceEvent<TKey, TResource>> TriggerOnIterator<TKey, TResource>(
            IInformable<TKey, TResource> source,
            ITrigger<TResource> trigger,
            CancellationToken cancellationToken)
        {
            var last = new Dictionary<TKey, object>(); // per key: the capture of the last version this consumer saw
            await foreach (var e in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var flags = e.EventFlags;
                if (flags.HasFlag(EventTypeFlags.ResetStart) || flags.HasFlag(EventTypeFlags.ResetEmpty))
                    last.Clear();
                if (flags.HasFlag(EventTypeFlags.ResetEmpty))
                {
                    yield return e;
                    continue;
                }

                if (flags.HasFlag(EventTypeFlags.Delete))
                {
                    last.Remove(e.Key);
                    yield return e;
                    continue;
                }

                var capture = trigger.Capture(e.Value);
                var quiet = false;
                if (flags.HasFlag(EventTypeFlags.Modify) && !flags.HasFlag(EventTypeFlags.Sync) && last.TryGetValue(e.Key, out var previous))
                    quiet = trigger.Changed(previous, capture) == null;
                last[e.Key] = capture;
                yield return quiet && !flags.IsQuiet() ? new ResourceEvent<TKey, TResource>(flags | EventTypeFlags.Quiet, e.Key, e.Value) : e;
            }
        }
    }
}
