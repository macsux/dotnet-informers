using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>Options of a patch computed from a change (<see cref="Patching.PatchAsync{T}(IKubernetesGenericClient,T,Action{T},PatchOptions,CancellationToken)" />).</summary>
    public sealed class PatchOptions
    {
        /// <summary>
        ///     Send the original's <c>resourceVersion</c> with the patch, so it fails with 409 Conflict if the object changed meanwhile (default).
        ///     A merge patch replaces lists whole (finalizers, conditions), which is safe only with this precondition. Set to false for
        ///     last-writer-wins on the touched fields.
        /// </summary>
        public bool RequireCurrentVersion { get; set; } = true;

        /// <summary>The field manager recorded for the write.</summary>
        public string FieldManager { get; set; }

        /// <summary>Validate and return the result without persisting it.</summary>
        public bool DryRun { get; set; }
    }

    /// <summary>
    ///     One way to change an object: edit a copy, and the change is sent as a JSON merge patch of only what differs. With a status subresource
    ///     the diff is split: <c>status</c> goes to <c>/status</c>, the rest to the object (the API server ignores each on the other endpoint).
    /// </summary>
    public static class Patching
    {
        /// <summary>
        ///     Clones <paramref name="obj" />, runs <paramref name="mutate" /> on the clone, and sends the difference (nothing when there is none).
        ///     Returns the object as stored after the patch (or <paramref name="obj" /> when nothing changed). The original is never modified, so
        ///     objects from an informer can be passed directly.
        /// </summary>
        public static Task<T> PatchAsync<T>(this IKubernetesGenericClient client, T obj, Action<T> mutate, PatchOptions options = null, CancellationToken cancellationToken = default)
            where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));
            if (mutate == null)
                throw new ArgumentNullException(nameof(mutate));
            var modified = obj.DeepClone();
            mutate(modified);
            return client.PatchAsync(obj, modified, options, cancellationToken);
        }

        /// <summary>
        ///     Sends the difference between <paramref name="original" /> and <paramref name="modified" /> (nothing when there is none) and returns
        ///     the object as stored after the patch (or <paramref name="original" /> when nothing changed).
        /// </summary>
        public static async Task<T> PatchAsync<T>(this IKubernetesGenericClient client, T original, T modified, PatchOptions options = null, CancellationToken cancellationToken = default)
            where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            if (original == null)
                throw new ArgumentNullException(nameof(original));
            if (modified == null)
                throw new ArgumentNullException(nameof(modified));
            options ??= new PatchOptions();
            var diff = Diff(original, modified);
            if (diff == null)
                return original;

            var writeOptions = new KubernetesWriteOptions { FieldManager = options.FieldManager, DryRun = options.DryRun };
            JsonNode statusPatch = null;
            var patchStatus = false;
            if (diff.ContainsKey("status") && (await client.GetResourceInfoAsync(original, cancellationToken).ConfigureAwait(false)).HasStatus)
            {
                patchStatus = true;
                statusPatch = diff["status"]; // null: status was removed
                diff.Remove("status");
            }

            var current = original;
            var version = original.Metadata?.ResourceVersion;
            if (diff.Count > 0)
            {
                current = await client.PatchAsync(current, Patch(diff, options.RequireCurrentVersion ? version : null), false, writeOptions, cancellationToken).ConfigureAwait(false);
                version = current.Metadata?.ResourceVersion;
            }

            if (patchStatus)
            {
                // guarded by the version the main patch produced: nobody else may have written in between
                var status = new JsonObject { ["status"] = statusPatch?.DeepClone() };
                current = await client.PatchAsync(current, Patch(status, options.RequireCurrentVersion ? version : null), true, writeOptions, cancellationToken).ConfigureAwait(false);
            }

            return current;
        }

        /// <summary>Saves the edits of a handle (see <see cref="Tracked{T}" />): sends the difference and makes the stored object its new original.</summary>
        public static async Task<T> SaveAsync<T>(this IKubernetesGenericClient client, Tracked<T> tracked, PatchOptions options = null, CancellationToken cancellationToken = default)
            where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (tracked == null)
                throw new ArgumentNullException(nameof(tracked));
            if (!tracked.IsModified)
                return tracked.Original;
            var saved = await client.PatchAsync(tracked.Original, tracked.Value, options, cancellationToken).ConfigureAwait(false);
            tracked.Accept(saved);
            return saved;
        }

        /// <summary>The merge patch from <paramref name="original" /> to <paramref name="modified" />, or null when they serialize the same.</summary>
        internal static JsonObject Diff<T>(T original, T modified) where T : class
        {
            var before = ToJson(original);
            var after = ToJson(modified);
            return JsonMergePatch.Create(before, after);
        }

        internal static JsonObject ToJson(object value) =>
            JsonNode.Parse(InformerJson.SerializeToUtf8Bytes(value)) as JsonObject ?? throw new ArgumentException("The object doesn't serialize to a JSON object", nameof(value));

        private static V1Patch Patch(JsonObject patch, string resourceVersion)
        {
            if (resourceVersion != null)
            {
                if (patch["metadata"] is not JsonObject metadata)
                    patch["metadata"] = metadata = new JsonObject();
                metadata["resourceVersion"] = resourceVersion;
            }

            return new V1Patch(patch.ToJsonString(), V1Patch.PatchType.MergePatch);
        }
    }
}
