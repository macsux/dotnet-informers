namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Deliberately broken variants of the shared cache, used only by the stress tests to show they detect the bug classes they're
    ///     meant to detect (mutation testing). <see cref="None" /> is the real design.
    /// </summary>
    internal enum FaultMode
    {
        None,

        /// <summary>Apply under the lock, release it, then mark cursors dirty in a second lock acquisition. Stays correct.</summary>
        MarkAfterApplySeparateLock,

        /// <summary>Mark cursors dirty first, release the lock, then apply the event. Loses updates.</summary>
        MarkBeforeApply,

        /// <summary>Snapshot the cache, release the lock, then add the cursor to the marked list (the old Rx SharedInformer race).</summary>
        NonAtomicRegistration,

        /// <summary>Per-key revision counter that restarts at 1 when a key is re-added after a delete.</summary>
        PerKeyRevision,
    }
}
