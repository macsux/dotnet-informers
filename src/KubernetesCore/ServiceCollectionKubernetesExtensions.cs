using System;
using k8s;
using Microsoft.Extensions.DependencyInjection;
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

        public static IServiceCollection AddKubernetesInformers(this IServiceCollection services)
        {
            services.AddTransient(typeof(KubernetesInformer<>));
            services.AddSingleton(typeof(IKubernetesInformer<>), typeof(KubernetesInformer<>));
            return services;
        }
    }
}
