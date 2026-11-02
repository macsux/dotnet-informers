using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Creating and deleting objects a controller owns, with owner references and <see cref="Expectations" /> handled.</summary>
    public static class OwnedObjects
    {
        /// <summary>
        ///     Creates <paramref name="child" /> with a controller owner reference to <paramref name="owner" />, recording the create in
        ///     <paramref name="expectations" /> (keyed by the owner's key) first; a failed create lowers it again.
        /// </summary>
        public static async Task<T> CreateOwnedAsync<T>(
            this IKubernetesGenericClient client,
            IKubernetesObject<V1ObjectMeta> owner,
            T child,
            Expectations expectations = null,
            KubernetesWriteOptions options = null,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            if (child == null)
                throw new ArgumentNullException(nameof(child));
            if (child.Metadata?.OwnerReferences?.Any(x => x.Uid == owner?.Metadata?.Uid) != true)
                child.WithOwnerReference(owner);
            var ownerKey = owner.ObjectKey();
            expectations?.ExpectCreations(ownerKey, 1);
            try
            {
                return await client.CreateAsync(child, options, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                expectations?.CreationFailed(ownerKey); // will never be observed
                throw;
            }
        }

        /// <summary>
        ///     Deletes <paramref name="child" /> (guarded by its UID), recording the delete in <paramref name="expectations" /> (keyed by
        ///     <paramref name="owner" />'s key) first; when it was already gone or the delete failed, the expectation is lowered again.
        /// </summary>
        /// <returns>False when the object was already gone</returns>
        public static async Task<bool> DeleteOwnedAsync<T>(
            this IKubernetesGenericClient client,
            IKubernetesObject<V1ObjectMeta> owner,
            T child,
            Expectations expectations = null,
            V1DeleteOptions options = null,
            CancellationToken cancellationToken = default)
            where T : IKubernetesObject<V1ObjectMeta>
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            var ownerKey = owner.ObjectKey();
            expectations?.ExpectDeletions(ownerKey, 1);
            try
            {
                var deleted = await client.DeleteAsync(child, options, cancellationToken).ConfigureAwait(false);
                if (!deleted)
                    expectations?.DeletionFailed(ownerKey);
                return deleted;
            }
            catch
            {
                expectations?.DeletionFailed(ownerKey);
                throw;
            }
        }
    }

    /// <summary>In-memory helpers for <c>status.conditions</c>; persisted by the normal save.</summary>
    public static class Conditions
    {
        /// <summary>
        ///     Sets the condition of <paramref name="type" />: adds it, or updates it in place. <c>lastTransitionTime</c> changes only when the status
        ///     value does, so an unchanged condition serializes the same and saving it sends nothing.
        /// </summary>
        /// <param name="conditions">The list (e.g. <c>status.Conditions</c>)</param>
        /// <param name="type">CamelCase condition type, e.g. <c>Ready</c></param>
        /// <param name="status"><c>True</c>, <c>False</c> or <c>Unknown</c></param>
        /// <param name="reason">CamelCase reason for the last transition (required by the API, defaults to <c>Reconciled</c>)</param>
        /// <param name="message">Human-readable details</param>
        /// <param name="observedGeneration">The <c>metadata.generation</c> the condition was computed from</param>
        /// <param name="now">The transition time (default: now)</param>
        /// <returns>The condition as set</returns>
        public static V1Condition Set(this IList<V1Condition> conditions, string type, string status, string reason = null, string message = null, long? observedGeneration = null, DateTime? now = null)
        {
            if (conditions == null)
                throw new ArgumentNullException(nameof(conditions));
            if (string.IsNullOrEmpty(type))
                throw new ArgumentNullException(nameof(type));
            if (status is not ("True" or "False" or "Unknown"))
                throw new ArgumentOutOfRangeException(nameof(status), status, "A condition's status is True, False or Unknown");
            var existing = conditions.Get(type);
            if (existing == null)
            {
                existing = new V1Condition { Type = type, Status = status, LastTransitionTime = Truncate(now ?? DateTime.UtcNow) };
                conditions.Add(existing);
            }
            else if (existing.Status != status)
            {
                existing.Status = status;
                existing.LastTransitionTime = Truncate(now ?? DateTime.UtcNow);
            }

            existing.Reason = reason ?? "Reconciled";
            existing.Message = message ?? string.Empty;
            existing.ObservedGeneration = observedGeneration;
            return existing;
        }

        /// <inheritdoc cref="Set(IList{V1Condition},string,string,string,string,long?,DateTime?)" />
        public static V1Condition Set(this IList<V1Condition> conditions, string type, bool status, string reason = null, string message = null, long? observedGeneration = null, DateTime? now = null) =>
            conditions.Set(type, status ? "True" : "False", reason, message, observedGeneration, now);

        /// <summary>The condition of <paramref name="type" />, or null.</summary>
        public static V1Condition Get(this IEnumerable<V1Condition> conditions, string type) => conditions?.FirstOrDefault(x => x.Type == type);

        /// <summary>Whether the condition of <paramref name="type" /> is <c>True</c>.</summary>
        public static bool IsTrue(this IEnumerable<V1Condition> conditions, string type) => conditions.Get(type)?.Status == "True";

        /// <summary>Removes the condition of <paramref name="type" />; false when there was none.</summary>
        public static bool Remove(this IList<V1Condition> conditions, string type)
        {
            var existing = conditions.Get(type);
            return existing != null && conditions.Remove(existing);
        }

        // the API stores seconds (RFC 3339): sub-second precision would make every save look like a change
        private static DateTime Truncate(DateTime value) => new DateTime(value.Ticks - value.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
    }
}
