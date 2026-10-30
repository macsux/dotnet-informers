using System;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.KubernetesCore
{
    public static class Extensions
    {
        public static IServiceCollection AddKubernetesClient(this IServiceCollection services, Func<KubernetesClientConfiguration> configProvider)
        {
            services.AddSingleton(_ => configProvider());
            services.AddSingleton<k8s.Kubernetes>(container => new k8s.Kubernetes(container.GetRequiredService<KubernetesClientConfiguration>()));
            services.AddSingleton<IKubernetes>(container => container.GetRequiredService<k8s.Kubernetes>());
            services.AddSingleton<IKubernetesGenericClient>(container => new KubernetesGenericClient(container.GetRequiredService<k8s.Kubernetes>()));

            return services;
        }

        /// <summary>
        ///     Registers the shared informer factory (<see cref="IKubernetesInformerFactory" />) and <see cref="IKubernetesInformer{TResource}" />
        ///     for every model type. Every resolution of an informer for the same type is the same shared informer.
        /// </summary>
        public static IServiceCollection AddKubernetesInformers(this IServiceCollection services)
        {
            services.TryAddSingleton<IKubernetesInformerFactory>(container => new KubernetesInformerFactory(
                container.GetRequiredService<IKubernetesGenericClient>(),
                container.GetServices<KubernetesInformerRegistration>(),
                container.GetService<ILoggerFactory>()));
            services.TryAddSingleton(typeof(IKubernetesInformer<>), typeof(FactoryInformer<>));
            return services;
        }

        /// <summary>
        ///     Registers the informers (see <see cref="AddKubernetesInformers" />) and sets up the informer of <typeparamref name="TResource" />:
        ///     a transform applied before caching, indexes, or the mutation detector.
        /// </summary>
        public static IServiceCollection AddKubernetesInformer<TResource>(this IServiceCollection services, Action<KubernetesInformerRegistration<TResource>> configure = null)
            where TResource : IKubernetesObject<V1ObjectMeta>
        {
            var registration = new KubernetesInformerRegistration<TResource>();
            configure?.Invoke(registration);
            services.AddSingleton<KubernetesInformerRegistration>(registration);
            return services.AddKubernetesInformers();
        }
    }
}
