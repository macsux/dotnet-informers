using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>Options of the <c>RunAsync</c> reconcile loop.</summary>
    public sealed class RunOptions
    {
        /// <summary>How many handlers run at once. Two changes to the same key never run in parallel. Default 1.</summary>
        public int MaxConcurrency { get; set; } = 1;

        /// <summary>Per-key delay before retrying after an exception or <see cref="ReconcileResult.Requeue" />. Default <see cref="InformersBase.Backoff.Default" />.</summary>
        public Backoff Backoff { get; set; }

        /// <summary>Optional overall limit on how fast handlers start.</summary>
        public RateLimit RateLimit { get; set; }

        /// <summary>Optional: handlers only run while this instance leads; losing leadership cancels running handlers.</summary>
        public ILeaderElection LeaderElection { get; set; }

        /// <summary>A name for logs, metrics and traces.</summary>
        public string Name { get; set; }

        /// <summary>Logs handler failures and retries.</summary>
        public ILogger Logger { get; set; }

        /// <summary>
        ///     Recognizes an optimistic-concurrency conflict (e.g. HTTP 409). A conflict is retried promptly with the key's latest state and
        ///     doesn't count toward its backoff. Defaults to the detectors registered by source packages (the Kubernetes package recognizes 409).
        /// </summary>
        public Func<Exception, bool> IsConflict { get; set; }

        internal static readonly List<Func<Exception, bool>> DefaultConflictDetectors = new List<Func<Exception, bool>>();

        internal static void RegisterConflictDetector(Func<Exception, bool> detector)
        {
            lock (DefaultConflictDetectors)
                DefaultConflictDetectors.Add(detector);
        }

        internal bool IsConflictException(Exception e)
        {
            if (IsConflict != null)
                return IsConflict(e);
            lock (DefaultConflictDetectors)
            {
                foreach (var detector in DefaultConflictDetectors)
                {
                    if (detector(e))
                        return true;
                }
            }

            return false;
        }
    }
}
