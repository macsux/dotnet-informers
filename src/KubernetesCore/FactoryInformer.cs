using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.KubernetesCore
{
    /// <summary>Resolves <see cref="IKubernetesInformer{TResource}" /> from the container to the factory's shared informer.</summary>
    internal sealed class FactoryInformer<TResource> : IKubernetesInformer<TResource>, ISharedSource<string, TResource>
        where TResource : IKubernetesObject<V1ObjectMeta>
    {
        private readonly IKubernetesInformer<TResource> _informer;

        public FactoryInformer(IKubernetesInformerFactory factory) => _informer = factory.Get<TResource>();

        public GroupVersionResource Resource => _informer.Resource;
        public PayloadShape Shape => _informer.Shape;
        public bool IsSynced => _informer.IsSynced;
        public Task WaitForSyncAsync(CancellationToken cancellationToken = default) => _informer.WaitForSyncAsync(cancellationToken);
        public IInformer<string, TResource> Where(Expression<Func<TResource, bool>> predicate) => _informer.Where(predicate);
        public IInformable<string, TResource> ListWatch() => _informer.ListWatch();
        public IInformer<string, TResource> ListWatch(KubernetesInformerOptions options) => _informer.ListWatch(options);
        public IAsyncEnumerable<TResource> List(CancellationToken cancellationToken = default) => _informer.List(cancellationToken);
        public IAsyncEnumerable<TResource> List(KubernetesInformerOptions options, CancellationToken cancellationToken = default) => _informer.List(options, cancellationToken);
        public IAsyncEnumerator<ResourceEvent<string, TResource>> GetAsyncEnumerator(CancellationToken cancellationToken = default) => _informer.GetAsyncEnumerator(cancellationToken);

        SharedSubscription<string, TResource> ISharedSource<string, TResource>.Subscribe(CancellationToken cancellationToken) =>
            ((ISharedSource<string, TResource>)_informer).Subscribe(cancellationToken);
    }
}
