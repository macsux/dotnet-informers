using System;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     A copy-on-write handle on an object handed out by an informer. <see cref="Original" /> is the shared instance and is never modified;
    ///     <see cref="Edit" /> clones it on first call and returns the same clone afterwards; <see cref="Value" /> is the clone once edited,
    ///     otherwise the original. Saving (<see cref="Patching.SaveAsync{T}" />) sends the difference as a patch and makes the stored object the
    ///     new original.
    /// </summary>
    public sealed class Tracked<T> where T : class
    {
        private T _edit;

        public Tracked(T original) => Original = original ?? throw new ArgumentNullException(nameof(original));

        /// <summary>The object as last read or saved (shared: never modify it).</summary>
        public T Original { get; private set; }

        /// <summary>The edited copy if <see cref="Edit" /> was called, otherwise <see cref="Original" />.</summary>
        public T Value => _edit ?? Original;

        /// <summary>Whether <see cref="Edit" /> was called since the last save (the edits may still amount to no change).</summary>
        public bool IsModified => _edit != null;

        /// <summary>The copy to change: cloned on first call, the same instance afterwards.</summary>
        public T Edit() => _edit ??= InformerJson.Clone(Original);

        /// <summary>Whether the edits change anything (serialize differently from the original).</summary>
        public bool HasChanges() => _edit != null && Patching.Diff(Original, _edit) != null;

        /// <summary>Drops the edits.</summary>
        public void Discard() => _edit = null;

        /// <summary>Makes <paramref name="saved" /> (the object as stored) the new original and drops the edits.</summary>
        public void Accept(T saved)
        {
            Original = saved ?? throw new ArgumentNullException(nameof(saved));
            _edit = null;
        }

        public override string ToString() => Value.ToString();
    }

    public static class Tracked
    {
        /// <summary>A copy-on-write handle on <paramref name="value" />.</summary>
        public static Tracked<T> Of<T>(T value) where T : class => new Tracked<T>(value);
    }
}
