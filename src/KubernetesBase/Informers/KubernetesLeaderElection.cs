using System;
using System.Threading;
using System.Threading.Tasks;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     Leader election through a <c>coordination.k8s.io/v1</c> <c>Lease</c>, for <see cref="RunOptions.LeaderElection" />, with client-go's
    ///     protocol: the holder renews the lease every <c>retryPeriod</c>; another replica takes it over once it saw no renewal for
    ///     <c>leaseDuration</c> (measured on its own clock from when it last saw the lease change, so clock skew between replicas doesn't
    ///     matter); a leader that can't renew within <c>renewDeadline</c> steps down. Starts campaigning on first use and keeps trying to
    ///     (re)acquire leadership until disposed, which releases a held lease.
    /// </summary>
    /// <remarks>Built on <see cref="IKubernetesGenericClient" /> (no dependency on KubernetesClient's leader election).</remarks>
    public sealed class KubernetesLeaderElection : ILeaderElection, IDisposable
    {
        private readonly object _lock = new object();
        private readonly IKubernetesGenericClient _client;
        private readonly string _namespace;
        private readonly string _leaseName;
        private readonly TimeSpan _leaseDuration;
        private readonly TimeSpan _renewDeadline;
        private readonly TimeSpan _retryPeriod;
        private readonly Func<DateTime> _clock;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private TaskCompletionSource<CancellationToken> _leading = NewWaiter();
        private CancellationTokenSource _term;
        private Task _campaign;
        private string _observedVersion;
        private DateTime _observedAt;

        /// <param name="kubernetes">The client whose connection the lease is managed with</param>
        /// <inheritdoc cref="KubernetesLeaderElection(IKubernetesGenericClient,string,string,string,TimeSpan?,TimeSpan?,TimeSpan?,ILogger)" />
        public KubernetesLeaderElection(
            k8s.Kubernetes kubernetes,
            string @namespace,
            string leaseName,
            string identity,
            TimeSpan? leaseDuration = null,
            TimeSpan? renewDeadline = null,
            TimeSpan? retryPeriod = null,
            ILogger logger = null)
            : this(new KubernetesGenericClient(kubernetes ?? throw new ArgumentNullException(nameof(kubernetes))), @namespace, leaseName, identity, leaseDuration, renewDeadline, retryPeriod, logger)
        {
        }

        /// <param name="client">The client to manage the lease with</param>
        /// <param name="namespace">Namespace of the lease</param>
        /// <param name="leaseName">Name of the lease, shared by every replica of the controller</param>
        /// <param name="identity">This replica's identity (e.g. the pod name)</param>
        /// <param name="leaseDuration">How long a lease is valid without renewal (default 15 s)</param>
        /// <param name="renewDeadline">How long the leader keeps trying to renew before giving up (default 10 s)</param>
        /// <param name="retryPeriod">How often to try to acquire or renew (default 2 s)</param>
        /// <param name="logger">Optional logging of leadership changes</param>
        public KubernetesLeaderElection(
            IKubernetesGenericClient client,
            string @namespace,
            string leaseName,
            string identity,
            TimeSpan? leaseDuration = null,
            TimeSpan? renewDeadline = null,
            TimeSpan? retryPeriod = null,
            ILogger logger = null)
            : this(client, @namespace, leaseName, identity, leaseDuration ?? TimeSpan.FromSeconds(15), renewDeadline ?? TimeSpan.FromSeconds(10), retryPeriod ?? TimeSpan.FromSeconds(2), logger,
                () => DateTime.UtcNow)
        {
        }

        internal KubernetesLeaderElection(
            IKubernetesGenericClient client,
            string @namespace,
            string leaseName,
            string identity,
            TimeSpan leaseDuration,
            TimeSpan renewDeadline,
            TimeSpan retryPeriod,
            ILogger logger,
            Func<DateTime> clock)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _namespace = string.IsNullOrEmpty(@namespace) ? throw new ArgumentNullException(nameof(@namespace)) : @namespace;
            _leaseName = string.IsNullOrEmpty(leaseName) ? throw new ArgumentNullException(nameof(leaseName)) : leaseName;
            Identity = string.IsNullOrEmpty(identity) ? throw new ArgumentNullException(nameof(identity)) : identity;
            if (renewDeadline >= leaseDuration)
                throw new ArgumentException("The renew deadline must be shorter than the lease duration", nameof(renewDeadline));
            _leaseDuration = leaseDuration;
            _renewDeadline = renewDeadline;
            _retryPeriod = retryPeriod;
            _logger = logger ?? NullLogger.Instance;
            _clock = clock;
        }

        private static TaskCompletionSource<CancellationToken> NewWaiter() => new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>This replica's identity in the lease.</summary>
        public string Identity { get; }

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
                _campaign ??= Task.Run(() => CampaignAsync(_stop.Token));
                leading = _leading.Task;
            }

            return leading.WaitAsync(cancellationToken);
        }

        private async Task CampaignAsync(CancellationToken cancellationToken)
        {
            var lastRenewal = DateTime.MinValue;
            while (!cancellationToken.IsCancellationRequested)
            {
                bool held;
                try
                {
                    held = await TryAcquireOrRenewAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Leader election for lease {Namespace}/{Lease} failed; retrying", _namespace, _leaseName);
                    held = false;
                }

                var now = _clock();
                if (held)
                {
                    lastRenewal = now;
                    StartLeading();
                }
                else if (IsLeader && now - lastRenewal >= _renewDeadline)
                {
                    StopLeading(); // couldn't renew in time (or someone else holds it): step down before the lease can expire
                }
                else if (!IsLeader)
                {
                    lastRenewal = DateTime.MinValue;
                }

                try
                {
                    await Task.Delay(_retryPeriod, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        /// <summary>One round: creates, renews or takes over the lease. True when this replica holds it afterwards.</summary>
        internal async Task<bool> TryAcquireOrRenewAsync(CancellationToken cancellationToken)
        {
            var now = _clock();
            var lease = await _client.GetAsync<V1Lease>(_leaseName, _namespace, cancellationToken).ConfigureAwait(false);
            if (lease == null)
            {
                try
                {
                    var created = await _client.CreateAsync(new V1Lease
                    {
                        ApiVersion = "coordination.k8s.io/v1",
                        Kind = "Lease",
                        Metadata = new V1ObjectMeta { Name = _leaseName, NamespaceProperty = _namespace },
                        Spec = new V1LeaseSpec
                        {
                            HolderIdentity = Identity,
                            LeaseDurationSeconds = (int)Math.Ceiling(_leaseDuration.TotalSeconds),
                            AcquireTime = now,
                            RenewTime = now,
                            LeaseTransitions = 0,
                        },
                    }, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Observe(created, now);
                    return true;
                }
                catch (Exception ex) when (ex.IsAlreadyExists())
                {
                    return false; // someone created it first
                }
            }

            if (lease.Metadata.ResourceVersion != _observedVersion)
                Observe(lease, now);
            var holder = lease.Spec?.HolderIdentity;
            var ours = holder == Identity;
            var expired = string.IsNullOrEmpty(holder) || now - _observedAt >= LeaseDuration(lease);
            if (!ours && !expired)
                return false;

            try
            {
                var renewed = await _client.PatchAsync(lease, l =>
                {
                    l.Spec ??= new V1LeaseSpec();
                    if (!ours)
                    {
                        l.Spec.HolderIdentity = Identity;
                        l.Spec.AcquireTime = now;
                        l.Spec.LeaseTransitions = (l.Spec.LeaseTransitions ?? 0) + 1;
                    }

                    l.Spec.LeaseDurationSeconds = (int)Math.Ceiling(_leaseDuration.TotalSeconds);
                    l.Spec.RenewTime = now;
                }, cancellationToken: cancellationToken).ConfigureAwait(false);
                Observe(renewed, now);
                return true;
            }
            catch (Exception ex) when (ex.IsConflict())
            {
                return false; // someone else renewed or took it in between
            }
        }

        private TimeSpan LeaseDuration(V1Lease lease) => lease.Spec?.LeaseDurationSeconds is { } seconds ? TimeSpan.FromSeconds(seconds) : _leaseDuration;

        private void Observe(V1Lease lease, DateTime now)
        {
            _observedVersion = lease.Metadata?.ResourceVersion;
            _observedAt = now;
        }

        private void StartLeading()
        {
            lock (_lock)
            {
                if (_term != null || _stop.IsCancellationRequested)
                    return;
                _term = new CancellationTokenSource();
                _leading.TrySetResult(_term.Token);
            }

            _logger.LogInformation("{Identity} became the leader of lease {Namespace}/{Lease}", Identity, _namespace, _leaseName);
        }

        private void StopLeading()
        {
            CancellationTokenSource term;
            lock (_lock)
            {
                term = _term;
                _term = null;
                if (_leading.Task.IsCompleted)
                    _leading = NewWaiter();
            }

            if (term == null)
                return;
            _logger.LogInformation("{Identity} lost the leadership of lease {Namespace}/{Lease}", Identity, _namespace, _leaseName);
            term.Cancel();
        }

        /// <summary>Stops campaigning; a held lease is released so another replica takes over without waiting for it to expire.</summary>
        public void Dispose()
        {
            bool wasLeader;
            Task campaign;
            lock (_lock)
            {
                if (_stop.IsCancellationRequested)
                    return;
                _stop.Cancel();
                wasLeader = _term != null;
                campaign = _campaign;
            }

            StopLeading();
            try
            {
                campaign?.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
            }

            if (wasLeader)
                ReleaseAsync().Wait(TimeSpan.FromSeconds(5));
        }

        private async Task ReleaseAsync()
        {
            try
            {
                var lease = await _client.GetAsync<V1Lease>(_leaseName, _namespace).ConfigureAwait(false);
                if (lease?.Spec?.HolderIdentity == Identity)
                {
                    await _client.PatchAsync(lease, l =>
                    {
                        l.Spec.HolderIdentity = null;
                        l.Spec.LeaseDurationSeconds = 1;
                    }).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Releasing lease {Namespace}/{Lease} failed; it expires on its own", _namespace, _leaseName);
            }
        }
    }
}
