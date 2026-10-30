using System.Threading;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    internal static class KubernetesConflictDetection
    {
        private static int _registered;

        /// <summary>
        ///     Makes <c>RunAsync</c> treat HTTP 409 Conflict from the Kubernetes client as a conflict (prompt retry with the latest state). Called
        ///     when the first Kubernetes informer is created.
        /// </summary>
        internal static void EnsureRegistered()
        {
            if (Interlocked.Exchange(ref _registered, 1) == 0)
                RunOptions.RegisterConflictDetector(e => e.IsConflict());
        }
    }
}
