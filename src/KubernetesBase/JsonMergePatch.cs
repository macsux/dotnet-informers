using System.Linq;
using System.Text.Json.Nodes;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Computes a JSON merge patch (RFC 7386) between two versions of a document: changed and added members carry their new value, removed
    ///     members are <c>null</c>, objects are diffed member by member and everything else (arrays, primitives) is replaced whole.
    /// </summary>
    internal static class JsonMergePatch
    {
        /// <summary>The patch turning <paramref name="original" /> into <paramref name="modified" />, or null when they're equal.</summary>
        public static JsonObject Create(JsonObject original, JsonObject modified)
        {
            JsonObject patch = null;
            foreach (var (name, value) in modified)
            {
                if (!original.TryGetPropertyValue(name, out var before))
                {
                    if (value != null) // null deletes, and it isn't there
                        (patch ??= new JsonObject())[name] = value.DeepClone();
                    continue;
                }

                if (before is JsonObject beforeObject && value is JsonObject afterObject)
                {
                    var nested = Create(beforeObject, afterObject);
                    if (nested != null)
                        (patch ??= new JsonObject())[name] = nested;
                }
                else if (!JsonNode.DeepEquals(before, value))
                {
                    (patch ??= new JsonObject())[name] = value?.DeepClone();
                }
            }

            foreach (var name in original.Select(x => x.Key))
            {
                if (!modified.ContainsKey(name))
                    (patch ??= new JsonObject())[name] = null;
            }

            return patch;
        }

        /// <summary>Applies a merge patch to <paramref name="target" /> in place.</summary>
        public static void Apply(JsonObject target, JsonObject patch)
        {
            foreach (var (name, value) in patch.ToList())
            {
                if (value == null)
                    target.Remove(name);
                else if (value is JsonObject nested && target[name] is JsonObject existing)
                    Apply(existing, nested);
                else
                    target[name] = value.DeepClone();
            }
        }
    }
}
