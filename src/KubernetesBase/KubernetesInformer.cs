using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.InformersBase.FaultTolerance;
using RetryPolicy = Steeltoe.Informers.InformersBase.FaultTolerance.RetryPolicy;

namespace Steeltoe.Informers.KubernetesBase
{
    /// <summary>
    ///     An implementation of Kubernetes informer that talks to Kubernetes API Server
    /// </summary>
    /// <typeparam name="TResource">The type of Kubernetes resource</typeparam>
    internal class KubernetesInformer<TResource> : IKubernetesInformer<TResource> where TResource : IKubernetesObject<V1ObjectMeta>
    {
        private readonly IKubernetesGenericClient _kubernetes;
        private readonly Func<bool> _restartOnCompletion;
        private readonly ILogger<KubernetesInformer<TResource>> _logger;
        private readonly RetryPolicy _retryPolicy;

        public KubernetesInformer(IKubernetesGenericClient kubernetes, RetryPolicy retryPolicy = null, ILogger<KubernetesInformer<TResource>> logger = null) : this(kubernetes, retryPolicy, () => true, logger)
        {
            
        }

        public KubernetesInformer(IKubernetesGenericClient kubernetes, RetryPolicy retryPolicy, Func<bool> restartOnCompletion, ILogger<KubernetesInformer<TResource>> logger = null)
        {
            _kubernetes = kubernetes;
            _restartOnCompletion = restartOnCompletion;
            _logger = logger ?? NullLogger<KubernetesInformer<TResource>>.Instance;
            _retryPolicy = retryPolicy ?? DefaultRetryPolicy;
            
        }

        private static RetryPolicy DefaultRetryPolicy => new RetryPolicy(
            (exception, retryAttempt) => exception.IsTransient(),
            retryAttempt => TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryAttempt), 30)));

        public IAsyncEnumerable<TResource> List(CancellationToken cancellationToken = default)
        {
            return List(KubernetesInformerOptions.Default, cancellationToken);
        }

        public IAsyncEnumerable<TResource> List(KubernetesInformerOptions options, CancellationToken cancellationToken = default)
        {
            return new KubernetesInformerEmitter(this, options).List(cancellationToken);
        }

        public IInformable<string, TResource> ListWatch() => ListWatch(KubernetesInformerOptions.Default);

        public IInformable<string, TResource> ListWatch(KubernetesInformerOptions options)
        {
            return new KubernetesInformerEmitter(this, options).ListWatch(CancellationToken.None);
        }

        private class KubernetesInformerEmitter
        {
            private readonly KubernetesInformerOptions _options;
            private readonly KubernetesInformer<TResource> _parent;
            private string _resourceVersion;

            public KubernetesInformerEmitter(KubernetesInformer<TResource> parent, KubernetesInformerOptions options)
            {
                _parent = parent;
                _options = options;
            }

            public async IAsyncEnumerable<TResource> List([EnumeratorCancellation] CancellationToken cancellationToken)
            {
                var response = await _parent._retryPolicy.ExecuteAsync(async () =>
                {
                    var httpResponse = await _parent._kubernetes.ListWithHttpMessagesAsync<TResource>(
                        _options.Namespace,
                        resourceVersion: _resourceVersion,
                        labelSelector: _options.LabelSelector,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                    httpResponse.EnsureSuccessStatusCode();

                    return httpResponse;
                });

                var listKubernetesObject = response.Body;
                _resourceVersion = listKubernetesObject.Metadata.ResourceVersion;
                var items = listKubernetesObject.Items ?? new List<TResource>();
                foreach (var item in items)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    yield return item;
                }
               
            }
            private IObservable<ResourceEvent<string, TResource>> List() =>
                Observable.Create<ResourceEvent<string, TResource>>(async (observer, cancellationToken) =>
                {
                    await foreach (var item in List(cancellationToken).ToReset(x => x.Metadata.Name, x => x, true, cancellationToken))
                    {
                        observer.OnNext(item);
                    }
                });

            public IInformable<string, TResource> ListWatch([EnumeratorCancellation] CancellationToken cancellationToken)
            {
                return ListWatch().ToInformable();
            }
            private IObservable<ResourceEvent<string, TResource>> ListWatch()
            {
                return List(CancellationToken.None)
                    .ToReset(x => x.Metadata.Name, x => x, true)
                    .ToObservable()
                    .Concat(Watch());
               
            }
            
            private IObservable<ResourceEvent<string, TResource>> Watch()
            {
                
                return Observable.Create<ResourceEvent<string, TResource>>(async (observer, cancellationToken) =>
                    {
                        var result = await _parent._kubernetes.ListWithHttpMessagesAsync<TResource>(
                            _options.Namespace,
                            watch: true,
                            allowWatchBookmarks: true,
                            resourceVersion: _resourceVersion,
                            labelSelector: _options.LabelSelector,
                            cancellationToken: cancellationToken
                        ).ConfigureAwait(false);
                        result.EnsureSuccessStatusCode();
                        return Task.FromResult(result)
                            .Watch()
                            .SelectMany(x => // this is not a one to one mapping as some events cause side effects but don't propagate, so we need SelectMany
                            {
                                if (x.Object is IMetadata<V1ObjectMeta> status && status.Metadata.ResourceVersion != null)
                                {
                                    _resourceVersion = status.Metadata.ResourceVersion;
                                }
                                switch (x.Type)
                                {
                                    case WatchEventType.Added:
                                        return new[] { x.Object.ToResourceEvent(EventTypeFlags.Add) };
                                    case WatchEventType.Deleted:
                                        return new[] { x.Object.ToResourceEvent(EventTypeFlags.Delete) };
                                    case WatchEventType.Modified:
                                        return new[] { x.Object.ToResourceEvent(EventTypeFlags.Modify) };
                                    case WatchEventType.Bookmark:
                                        // we're just updating resource version
                                        break;
                                    case WatchEventType.Error:
                                    default:
                                        if (x.Object is V1Status error)
                                        {
                                            throw new KubernetesException(error);
                                        }

                                        throw new KubernetesException($"Received unknown error in watch: {x.Object}");
                                }

                                return Enumerable.Empty<ResourceEvent<string, TResource>>();
                            })
                            .Select(x => x)
                            // watch should never "complete" on it's own unless there's a critical exception, except in testing scenarios
                            .Concat(_parent._restartOnCompletion() ? Observable.Defer(Watch) : Observable.Empty<ResourceEvent<string, TResource>>())
                            .Subscribe(observer);
                    })
                    .Catch<ResourceEvent<string, TResource>, Exception>(exception =>
                    {
                        _parent._logger.LogError(exception.Message, exception);
                        // handle case when we tried rewatching by specifying resource version to resume after disconnect,
                        // but resource is too stale - should try to resubscribe from scratch
                        if (exception.IsResourceVersionGone() && _resourceVersion != null)
                        {
                            // we tried resuming but failed, restart from scratch
                            _resourceVersion = null;
                            return ListWatch();
                        }

                        return Observable.Throw<ResourceEvent<string, TResource>>(exception);
                    })
                    .WithRetryPolicy(_parent._retryPolicy);
            }
        }
    }
}
