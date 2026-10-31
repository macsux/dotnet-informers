using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;
using Steeltoe.Informers.KubernetesCore;

namespace Steeltoe.Informers.Operator
{
    /// <summary>Settings shared by every controller of the operator.</summary>
    public sealed class OperatorOptions
    {
        internal string DefaultNamespace { get; private set; }
        internal (string LeaseName, string Namespace, string Identity)? Leader { get; private set; }
        internal KubernetesEventRecorderOptions EventOptions { get; } = new KubernetesEventRecorderOptions();
        internal bool EventsEnabled { get; private set; } = true;
        internal bool AutomaticEventsEnabled { get; private set; } = true;
        internal TimeSpan SyncTimeout { get; private set; } = TimeSpan.FromMinutes(1);
        internal string ManifestOutput { get; } = Environment.GetEnvironmentVariable(RbacManifest.OutputVariable) is { Length: > 0 } output ? output : null;
        internal string RbacRoleName { get; private set; } = "operator";
        internal (System.Reflection.Assembly Assembly, bool Overwrite)? CrdInstall { get; set; }

        /// <summary>Adds the app's source-generated JSON metadata for the types it watches (see <see cref="InformerJson" />).</summary>
        public OperatorOptions Json(IJsonTypeInfoResolver resolver)
        {
            InformerJson.AddResolver(resolver);
            return this;
        }

        /// <summary>The namespace every controller watches unless it says otherwise (default: all namespaces).</summary>
        public OperatorOptions Namespace(string @namespace)
        {
            DefaultNamespace = @namespace;
            return this;
        }

        /// <summary>
        ///     Only the replica holding the <c>Lease</c> <paramref name="leaseName" /> in <paramref name="namespace" /> reconciles; every replica
        ///     keeps its informers warm. The identity defaults to the <c>POD_NAME</c> environment variable, or the machine name.
        /// </summary>
        public OperatorOptions LeaderElection(string leaseName, string @namespace, string identity = null)
        {
            Leader = (leaseName ?? throw new ArgumentNullException(nameof(leaseName)), @namespace ?? throw new ArgumentNullException(nameof(@namespace)),
                identity ?? Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName);
            return this;
        }

        /// <summary>Configures Kubernetes Events (API version, reporting controller, aggregation, rate limits).</summary>
        public OperatorOptions Events(Action<KubernetesEventRecorderOptions> configure)
        {
            configure?.Invoke(EventOptions);
            return this;
        }

        /// <summary>Doesn't write Kubernetes Events (they're logged instead), for control planes without the <c>Event</c> resource.</summary>
        public OperatorOptions DisableEvents()
        {
            EventsEnabled = false;
            return this;
        }

        /// <summary>Whether a reconcile error records a <c>ReconcileError</c> warning and a completed finalizer a <c>Finalized</c> event (default true).</summary>
        public OperatorOptions AutomaticEvents(bool enabled)
        {
            AutomaticEventsEnabled = enabled;
            return this;
        }

        /// <summary>The name of the generated <c>ClusterRole</c> (default <c>operator</c>).</summary>
        public OperatorOptions RbacRole(string name)
        {
            RbacRoleName = string.IsNullOrEmpty(name) ? throw new ArgumentNullException(nameof(name)) : name;
            return this;
        }

        /// <summary>How long startup waits for the controllers' informers to sync before going on regardless (default one minute).</summary>
        public OperatorOptions WaitForSync(TimeSpan timeout)
        {
            SyncTimeout = timeout;
            return this;
        }
    }

    /// <summary>Registers controllers with the operator.</summary>
    public sealed class OperatorBuilder
    {
        private readonly OperatorOptions _options;

        internal OperatorBuilder(IServiceCollection services, OperatorOptions options)
        {
            Services = services;
            _options = options;
        }

        public IServiceCollection Services { get; }

        /// <summary>
        ///     Development only: before the controllers start, creates the CustomResourceDefinitions embedded as YAML resources in
        ///     <paramref name="assembly" /> (default: the entry assembly), e.g. the generated <c>deploy/crds/*.yaml</c> added as
        ///     <c>EmbeddedResource</c>. Existing ones are replaced only with <paramref name="overwriteExisting" />. Needs permission to manage CRDs,
        ///     which an operator shouldn't have in production; parses YAML with KubernetesClient, so it isn't available in NativeAOT apps.
        /// </summary>
        public OperatorBuilder InstallCrds(System.Reflection.Assembly assembly = null, bool overwriteExisting = false)
        {
            _options.CrdInstall = (assembly ?? System.Reflection.Assembly.GetEntryAssembly(), overwriteExisting);
            return this;
        }

        /// <summary>Registers a controller class (resolved with a scope per reconcile).</summary>
        public OperatorBuilder AddController<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TController>()
            where TController : class, IController
        {
            Services.TryAddScoped<TController>();
            Services.AddSingleton<ControllerRegistration>(new ClassControllerRegistration<TController>());
            return this;
        }

        /// <summary>Registers a controller without a class: <paramref name="configure" /> declares it, including its <c>Reconcile</c>.</summary>
        public OperatorBuilder AddController<T>(Action<ControllerBuilder<T>> configure) where T : class, IKubernetesObject<V1ObjectMeta>
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));
            Services.AddSingleton<ControllerRegistration>(new DelegateControllerRegistration<T>(configure));
            return this;
        }
    }

    /// <summary>What a controller is, without running it: its name and the permissions it needs.</summary>
    internal sealed record ControllerDescription(string Name, IReadOnlyList<(GroupVersionResource Resource, IReadOnlyList<string> Verbs)> Permissions);

    internal abstract class ControllerRegistration
    {
        public abstract IControllerRunner CreateRunner(IServiceProvider services, ControllerServices controllerServices);

        /// <summary>Runs only the controller's <c>Configure</c> (no cluster needed).</summary>
        public abstract ControllerDescription Describe(IServiceProvider services);
    }

    internal sealed class ClassControllerRegistration<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TController> : ControllerRegistration
        where TController : class, IController
    {
        public override IControllerRunner CreateRunner(IServiceProvider services, ControllerServices controllerServices)
        {
            using var scope = services.GetRequiredService<IServiceScopeFactory>().CreateScope();
            return scope.ServiceProvider.GetRequiredService<TController>().CreateRunner(services, controllerServices, typeof(TController));
        }

        public override ControllerDescription Describe(IServiceProvider services)
        {
            using var scope = services.GetRequiredService<IServiceScopeFactory>().CreateScope();
            return scope.ServiceProvider.GetRequiredService<TController>().Describe(typeof(TController));
        }
    }

    internal sealed class DelegateControllerRegistration<T> : ControllerRegistration where T : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly Action<ControllerBuilder<T>> _configure;

        public DelegateControllerRegistration(Action<ControllerBuilder<T>> configure) => _configure = configure;

        public override IControllerRunner CreateRunner(IServiceProvider services, ControllerServices controllerServices)
        {
            var builder = new ControllerBuilder<T>(typeof(T).Name);
            _configure(builder);
            return new ControllerRunner<T>(builder, controllerServices, builder.ReconcileDelegate, builder.DeletedDelegate);
        }

        public override ControllerDescription Describe(IServiceProvider services)
        {
            var builder = new ControllerBuilder<T>(typeof(T).Name);
            _configure(builder);
            return new ControllerDescription(builder.ControllerName, builder.Permissions());
        }
    }

    /// <summary>The operator's controllers, for health checks and RBAC generation.</summary>
    public sealed class OperatorStatus
    {
        private readonly Lazy<IReadOnlyList<IControllerRunner>> _runners;
        private readonly Lazy<IReadOnlyList<ControllerDescription>> _descriptions;

        internal OperatorStatus(Lazy<IReadOnlyList<IControllerRunner>> runners, Lazy<IReadOnlyList<ControllerDescription>> descriptions)
        {
            _runners = runners;
            _descriptions = descriptions;
        }

        internal IReadOnlyList<IControllerRunner> Runners => _runners.Value;

        /// <summary>The names of the registered controllers.</summary>
        public IReadOnlyList<string> Controllers => _descriptions.Value.Select(x => x.Name).ToList();

        /// <summary>Whether every controller's informers delivered their initial state.</summary>
        public bool IsSynced => Runners.All(x => x.Synced.IsCompleted);

        /// <summary>Completes once every controller's informers delivered their initial state.</summary>
        public Task WhenSyncedAsync(CancellationToken cancellationToken = default) => Task.WhenAll(Runners.Select(x => x.Synced)).WaitAsync(cancellationToken);

        /// <summary>The resources the controllers read and write, with the verbs they need (the basis of their RBAC rules).</summary>
        public IReadOnlyList<(GroupVersionResource Resource, IReadOnlyList<string> Verbs)> Resources =>
            _descriptions.Value.SelectMany(x => x.Permissions)
                .GroupBy(x => x.Resource)
                .Select(x => (x.Key, (IReadOnlyList<string>)x.SelectMany(r => r.Verbs).Distinct().ToList()))
                .ToList();
    }

    /// <summary>
    ///     Runs every controller in one hosted service. Startup waits (bounded) for their informers to sync; a controller that fails restarts
    ///     with backoff and is logged, without taking the others down; shutdown stops starting reconciles and waits for running ones.
    /// </summary>
    internal sealed class OperatorHostedService : BackgroundService
    {
        private readonly OperatorStatus _status;
        private readonly OperatorOptions _options;
        private readonly ILogger _logger;

        private readonly IKubernetesGenericClient _client;

        public OperatorHostedService(OperatorStatus status, OperatorOptions options, IKubernetesGenericClient client, ILoggerFactory loggerFactory = null)
        {
            _status = status;
            _options = options;
            _client = client;
            _logger = loggerFactory?.CreateLogger("Steeltoe.Informers.Operator") ?? NullLogger.Instance;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_options.ManifestOutput != null)
                return; // manifest mode: ManifestWriterService writes them and stops the app
            if (_options.CrdInstall is { } install)
                await CrdInstaller.InstallAsync(_client, install.Assembly, install.Overwrite, _logger, cancellationToken).ConfigureAwait(false);
            await base.StartAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _status.WhenSyncedAsync(cancellationToken).WaitAsync(_options.SyncTimeout, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Controllers {Controllers} didn't sync within {Timeout}; starting anyway",
                    string.Join(", ", _status.Runners.Where(x => !x.Synced.IsCompleted).Select(x => x.Name)), _options.SyncTimeout);
            }
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.WhenAll(_status.Runners.Select(x => SuperviseAsync(x, stoppingToken)));

        private async Task SuperviseAsync(IControllerRunner runner, CancellationToken stoppingToken)
        {
            var failures = 0;
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await runner.RunAsync(stoppingToken).ConfigureAwait(false);
                    if (stoppingToken.IsCancellationRequested)
                        return;
                    _logger.LogWarning("Controller {Controller} stopped unexpectedly; restarting", runner.Name);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    failures++;
                    _logger.LogError(ex, "Controller {Controller} failed; restarting (attempt {Attempt})", runner.Name, failures);
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, Math.Min(failures, 6)))), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    public static class OperatorServiceCollectionExtensions
    {
        /// <summary>
        ///     Adds the operator: shared informers, Kubernetes Events, leader election (if configured) and one hosted service running every
        ///     controller registered on the returned builder. Needs a Kubernetes client (<c>AddKubernetesClient</c>).
        /// </summary>
        public static OperatorBuilder AddKubernetesOperator(this IServiceCollection services, Action<OperatorOptions> configure = null)
        {
            var options = new OperatorOptions();
            configure?.Invoke(options);
            services.AddKubernetesInformers();
            services.TryAddSingleton(options);
            services.TryAddSingleton<IEventRecorder>(container => options.EventsEnabled
                ? new KubernetesEventRecorder(container.GetRequiredService<IKubernetesGenericClient>(), options.EventOptions, container.GetService<ILoggerFactory>()?.CreateLogger("Steeltoe.Informers.Events"))
                : new LoggingEventRecorder(container.GetService<ILoggerFactory>()?.CreateLogger("Steeltoe.Informers.Events") ?? NullLogger.Instance));
            services.TryAddSingleton(container => new OperatorStatus(
                // the runners need a cluster; descriptions (names, RBAC) only run Configure
                new Lazy<IReadOnlyList<IControllerRunner>>(() =>
                {
                    var controllerServices = CreateControllerServices(container, options);
                    return container.GetServices<ControllerRegistration>().Select(x => x.CreateRunner(container, controllerServices)).ToList();
                }),
                new Lazy<IReadOnlyList<ControllerDescription>>(() => container.GetServices<ControllerRegistration>().Select(x => x.Describe(container)).ToList())));
            services.AddHostedService<ManifestWriterService>();
            services.AddHostedService<OperatorHostedService>();
            return new OperatorBuilder(services, options);
        }

        private static ControllerServices CreateControllerServices(IServiceProvider container, OperatorOptions options)
        {
            var leader = options.Leader;
            var loggerFactory = container.GetService<ILoggerFactory>();
            return new ControllerServices
            {
                Informers = container.GetRequiredService<IKubernetesInformerFactory>(),
                Client = container.GetRequiredService<IKubernetesGenericClient>(),
                Events = container.GetRequiredService<IEventRecorder>(),
                LoggerFactory = loggerFactory,
                DefaultNamespace = options.DefaultNamespace,
                LeaderElection = leader == null
                    ? null
                    : new KubernetesLeaderElection(container.GetRequiredService<IKubernetes>(), leader.Value.Namespace, leader.Value.LeaseName, leader.Value.Identity,
                        logger: loggerFactory?.CreateLogger("Steeltoe.Informers.LeaderElection")),
                AutomaticEvents = options.AutomaticEventsEnabled,
            };
        }
    }
}
