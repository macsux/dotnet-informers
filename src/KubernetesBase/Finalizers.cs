using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Finalizers: names in <c>metadata.finalizers</c> that keep Kubernetes from deleting an object until the controller that put them there
    ///     cleaned up and removed them. Adding and removing them are ordinary patches (<see cref="Patching" />), guarded by
    ///     <c>resourceVersion</c>.
    /// </summary>
    public static class Finalizers
    {
        /// <summary>Whether the object carries the finalizer.</summary>
        public static bool HasFinalizer(this IKubernetesObject<V1ObjectMeta> obj, string finalizer) => obj?.Metadata?.Finalizers?.Contains(finalizer) == true;

        /// <summary>Adds the finalizers the object doesn't carry yet and returns the object as stored (unchanged when nothing was missing).</summary>
        public static Task<T> AddFinalizersAsync<T>(this IKubernetesGenericClient client, T obj, IEnumerable<string> finalizers, CancellationToken cancellationToken = default)
            where T : class, IKubernetesObject<V1ObjectMeta> =>
            client.PatchAsync(obj, o =>
            {
                var list = o.Metadata.Finalizers ??= new List<string>();
                foreach (var finalizer in finalizers)
                {
                    if (!list.Contains(finalizer))
                        list.Add(finalizer);
                }
            }, cancellationToken: cancellationToken);

        /// <summary>
        ///     Removes a finalizer and returns the object as stored, or null when removing the last finalizer let the deletion complete (the object
        ///     is gone, which counts as success).
        /// </summary>
        public static async Task<T> RemoveFinalizerAsync<T>(this IKubernetesGenericClient client, T obj, string finalizer, CancellationToken cancellationToken = default)
            where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (!obj.HasFinalizer(finalizer))
                return obj;
            try
            {
                return await client.PatchAsync(obj, o => o.Metadata.Finalizers.Remove(finalizer), cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex.IsNotFound())
            {
                return null;
            }
        }

        /// <summary>
        ///     Wraps a reconcile handler of a raw pipeline with finalizers. For the object <paramref name="select" /> picks from each change:
        ///     <list type="bullet">
        ///         <item>not being deleted: the missing finalizers are added first, then <paramref name="reconcile" /> runs (with the change as it
        ///         arrived: a write guarded by its <c>resourceVersion</c> may conflict once and is retried with the latest state);</item>
        ///         <item>being deleted: the cleanup of every declared finalizer it still carries runs instead, in declaration order. <c>Done</c>
        ///         removes that finalizer; anything else keeps it and retries like a reconcile. Objects that never had them aren't cleaned up.</item>
        ///     </list>
        ///     Deletes go to <paramref name="reconcile" />.
        /// </summary>
        public static Func<ResourceEvent<TKey, TValue>, CancellationToken, Task<ReconcileResult>> Handle<TKey, TValue, T>(
            IKubernetesGenericClient client,
            Func<TValue, T> select,
            Func<ResourceEvent<TKey, TValue>, CancellationToken, Task<ReconcileResult>> reconcile,
            params (string Name, Func<ResourceEvent<TKey, TValue>, CancellationToken, Task<ReconcileResult>> Cleanup)[] finalizers)
            where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            if (select == null)
                throw new ArgumentNullException(nameof(select));
            if (reconcile == null)
                throw new ArgumentNullException(nameof(reconcile));
            if (finalizers == null || finalizers.Length == 0)
                throw new ArgumentException("At least one finalizer is needed", nameof(finalizers));
            var names = finalizers.Select(x => x.Name).ToList();
            return async (e, ct) =>
            {
                var obj = e.EventFlags.HasFlag(EventTypeFlags.Delete) || e.Value == null ? null : select(e.Value);
                if (obj == null)
                    return await reconcile(e, ct).ConfigureAwait(false);
                if (!obj.IsBeingDeleted())
                {
                    if (names.Any(x => !obj.HasFinalizer(x)))
                        await client.AddFinalizersAsync(obj, names, ct).ConfigureAwait(false);
                    return await reconcile(e, ct).ConfigureAwait(false);
                }

                foreach (var (name, cleanup) in finalizers)
                {
                    if (!obj.HasFinalizer(name))
                        continue;
                    var result = await cleanup(e, ct).ConfigureAwait(false);
                    if (!result.IsDone)
                        return result;
                    obj = await client.RemoveFinalizerAsync(obj, name, ct).ConfigureAwait(false);
                    if (obj == null)
                        break; // the deletion completed
                }

                return ReconcileResult.Done;
            };
        }
    }
}
