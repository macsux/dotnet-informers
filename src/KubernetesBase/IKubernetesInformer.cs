using k8s;
using Steeltoe.Informers.InformersBase;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     A shared informer over a Kubernetes resource. Objects are keyed <c>namespace/name</c> (<c>name</c> when cluster-scoped). Terms of
    ///     <c>Where</c> predicates on the namespace, name, labels and supported fields are pushed down to the API server; consumers whose
    ///     predicates translate to the same selectors share one list/watch connection.
    /// </summary>
    /// <typeparam name="TResource">The type of Kubernetes resource</typeparam>
    public interface IKubernetesInformer<TResource> :
        IInformer<string, TResource, KubernetesInformerOptions>,
        IInformer<string, TResource>
        where TResource : IKubernetesObject
    {
        /// <summary>The resource this informer watches.</summary>
        GroupVersionResource Resource { get; }

        /// <summary>What it receives for each object.</summary>
        PayloadShape Shape { get; }
    }
}
