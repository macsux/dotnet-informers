using System;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.LeaderElection;
using k8s.LeaderElection.ResourceLock;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Leader election through a Kubernetes <c>Lease</c> (KubernetesClient's <see cref="LeaderElector" />), for
    ///     <see cref="RunOptions.LeaderElection" />. Starts campaigning on first use and keeps trying to (re)acquire leadership until disposed.
    /// </summary>
    public sealed class KubernetesLeaderElection : ILeaderElection, IDisposable
    {
        private readonly object _lock = new object();
        private readonly LeaderElectionConfig _config;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private TaskCompletionSource<CancellationToken> _leading = NewWaiter();
        private CancellationTokenSource _term;
        private LeaderElector _elector;
        private Task _campaign;

        /// <param name="kubernetes">The client to manage the lease with</param>
        /// <param name="namespace">Namespace of the lease</param>
        /// <param name="leaseName">Name of the lease, shared by every replica of the controller</param>
        /// <param name="identity">This replica's identity (e.g. the pod name)</param>
        /// <param name="leaseDuration">How long a lease is valid without renewal (default 15 s)</param>
        /// <param name="renewDeadline">How long the leader keeps trying to renew before giving up (default 10 s)</param>
        /// <param name="retryPeriod">How often to try to acquire or renew (default 2 s)</param>
        /// <param name="logger">Optional logging of leadership changes</param>
        public KubernetesLeaderElection(
            IKubernetes kubernetes,
            string @namespace,
            string leaseName,
            string identity,
            TimeSpan? leaseDuration = null,
            TimeSpan? renewDeadline = null,
            TimeSpan? retryPeriod = null,
            ILogger logger = null)
            : this(new LeaderElectionConfig(new LeaseLock(kubernetes, @namespace, leaseName, identity))
            {
                LeaseDuration = leaseDuration ?? TimeSpan.FromSeconds(15),
                RenewDeadline = renewDeadline ?? TimeSpan.FromSeconds(10),
                RetryPeriod = retryPeriod ?? TimeSpan.FromSeconds(2),
            }, logger)
        {
        }

        public KubernetesLeaderElection(LeaderElectionConfig config, ILogger logger = null)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? NullLogger.Instance;
        }

        private static TaskCompletionSource<CancellationToken> NewWaiter() => new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Whether this instance currently leads.</summary>
        public bool IsLeader
        {
            get
            {
                lock (_lock)
                    return _term != null && !_term.IsCancellationRequested;
            }
        }

        public Task<CancellationToken> WaitForLeadershipAsync(CancellationToken cancellationToken)
        {
            Task<CancellationToken> leading;
            lock (_lock)
            {
                if (_stop.IsCancellationRequested)
                    throw new ObjectDisposedException(nameof(KubernetesLeaderElection));
                if (_campaign == null)
                {
                    _elector = new LeaderElector(_config);
                    _elector.OnStartedLeading += OnStartedLeading;
                    _elector.OnStoppedLeading += OnStoppedLeading;
                    _elector.OnError += e => _logger.LogWarning(e, "Leader election error for {Lock}", _config.Lock.Describe());
                    _campaign = Task.Run(() => _elector.RunAndTryToHoldLeadershipForeverAsync(_stop.Token));
                }

                leading = _leading.Task;
            }

            return leading.WaitAsync(cancellationToken);
        }

        private void OnStartedLeading()
        {
            _logger.LogInformation("{Identity} became the leader of {Lock}", _config.Lock.Identity, _config.Lock.Describe());
            lock (_lock)
            {
                _term = new CancellationTokenSource();
                _leading.TrySetResult(_term.Token);
            }
        }

        private void OnStoppedLeading()
        {
            _logger.LogInformation("{Identity} lost the leadership of {Lock}", _config.Lock.Identity, _config.Lock.Describe());
            CancellationTokenSource term;
            lock (_lock)
            {
                term = _term;
                _term = null;
                if (_leading.Task.IsCompleted)
                    _leading = NewWaiter();
            }

            term?.Cancel();
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_stop.IsCancellationRequested)
                    return;
                _stop.Cancel();
            }

            OnStoppedLeading();
            _elector?.Dispose();
        }
    }
}
