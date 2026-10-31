using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.Operator
{
    /// <summary>
    ///     A controller for one primary type: <see cref="Configure" /> declares its relationships and options once, <see cref="ReconcileAsync" />
    ///     brings one object's world to its desired state. It compiles to shared informers, joins, <c>Expectations</c> and <c>RunAsync</c>;
    ///     everything the reconcile reads comes from their caches.
    /// </summary>
    /// <remarks>
    ///     Instances are resolved from dependency injection with a scope per reconcile (so they can take scoped dependencies); <see cref="Configure" />
    ///     runs on one instance at startup.
    /// </remarks>
    public abstract class Controller<T> : IController where T : class, IKubernetesObject<V1ObjectMeta>
    {
        /// <summary>Declares relationships (<c>Owns</c>, <c>References</c>, <c>Watches</c>), finalizers, triggers and options.</summary>
        public virtual void Configure(ControllerBuilder<T> controller)
        {
        }

        /// <summary>
        ///     Reconciles one object. Runs at most once at a time per object; retried with backoff when it throws or returns
        ///     <see cref="ReconcileResult.Requeue" />, promptly with the latest state after a 409 Conflict.
        /// </summary>
        public abstract Task<ReconcileResult> ReconcileAsync(ReconcileContext<T> context, CancellationToken cancellationToken);

        /// <summary>
        ///     Called once an object is gone, with the last state seen. For cleanup outside the cluster prefer a finalizer, which runs before the
        ///     object disappears and is retried until it succeeds.
        /// </summary>
        public virtual Task DeletedAsync(string key, T lastKnown, CancellationToken cancellationToken) => Task.CompletedTask;

        IControllerRunner IController.CreateRunner(IServiceProvider services, ControllerServices controllerServices, Type controllerType)
        {
            var builder = new ControllerBuilder<T>(controllerType.Name);
            Configure(builder);
            var scopes = services.GetRequiredService<IServiceScopeFactory>();
            // cleanups declared as methods of this controller run, like the reconcile, on the instance of the reconcile's scope
            for (var i = 0; i < builder.Finalizers.Count; i++)
            {
                var (name, cleanup) = builder.Finalizers[i];
                if (!ReferenceEquals(cleanup.Target, this))
                    continue;
                var method = cleanup.Method;
                builder.Finalizers[i] = (name, async (context, ct) =>
                {
                    await using var scope = scopes.CreateAsyncScope();
                    var controller = scope.ServiceProvider.GetRequiredService(controllerType);
                    var bound = (Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>>)Delegate.CreateDelegate(typeof(Func<ReconcileContext<T>, CancellationToken, Task<ReconcileResult>>), controller, method);
                    return await bound(context, ct).ConfigureAwait(false);
                });
            }

            return new ControllerRunner<T>(builder, controllerServices,
                async (context, ct) =>
                {
                    await using var scope = scopes.CreateAsyncScope();
                    var controller = (Controller<T>)scope.ServiceProvider.GetRequiredService(controllerType);
                    return await controller.ReconcileAsync(context, ct).ConfigureAwait(false);
                },
                async (key, last, ct) =>
                {
                    await using var scope = scopes.CreateAsyncScope();
                    var controller = (Controller<T>)scope.ServiceProvider.GetRequiredService(controllerType);
                    await controller.DeletedAsync(key, last, ct).ConfigureAwait(false);
                });
        }
    }

    /// <summary>A controller class (derive from <see cref="Controller{T}" />).</summary>
    public interface IController
    {
        internal IControllerRunner CreateRunner(IServiceProvider services, ControllerServices controllerServices, Type controllerType);
    }
}
