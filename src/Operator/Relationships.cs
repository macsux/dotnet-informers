using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using k8s;
using k8s.Models;
using Steeltoe.Informers.InformersBase;
using Steeltoe.Informers.KubernetesBase;

namespace Steeltoe.Informers.Operator
{
    public enum RelationshipKind
    {
        /// <summary>Children with a controller owner reference to the primary.</summary>
        Owns,

        /// <summary>Objects the primary names.</summary>
        References,

        /// <summary>Objects that name primaries.</summary>
        Watches,
    }

    /// <summary>One primary with the related objects of each relationship, as a join produced them.</summary>
    internal sealed class ControllerState<T>
    {
        private readonly IReadOnlyList<object>[] _related;

        public ControllerState(T primary, int relationships)
        {
            Primary = primary;
            _related = new IReadOnlyList<object>[relationships];
        }

        private ControllerState(T primary, IReadOnlyList<object>[] related)
        {
            Primary = primary;
            _related = related;
        }

        public T Primary { get; }

        public IReadOnlyList<object> Related(int index) => _related[index] ?? Array.Empty<object>();

        public ControllerState<T> With(int index, IReadOnlyList<object> items)
        {
            var related = (IReadOnlyList<object>[])_related.Clone();
            related[index] = items;
            return new ControllerState<T>(Primary, related);
        }
    }

    /// <summary>What relationships need to build their part of the pipeline.</summary>
    internal sealed class ControllerScope
    {
        public IKubernetesInformerFactory Informers { get; init; }
        public IKubernetesGenericClient Client { get; init; }
        public string Namespace { get; init; }
        public OwnWrites OwnWrites { get; init; }
        public Expectations Expectations { get; init; }
        public string PrimaryApiVersion { get; init; }
        public string PrimaryKind { get; init; }
    }

    internal abstract class Relationship<T> where T : class, IKubernetesObject<V1ObjectMeta>
    {
        protected Relationship(int index, RelationshipKind kind, Type otherType, string name)
        {
            Index = index;
            Kind = kind;
            OtherType = otherType;
            Name = name;
        }

        public int Index { get; }
        public RelationshipKind Kind { get; }
        public Type OtherType { get; }
        public string Name { get; }

        /// <summary>Joins the related objects into the state.</summary>
        public abstract Task<IInformable<string, ControllerState<T>>> JoinAsync(IInformable<string, ControllerState<T>> state, ControllerScope scope, CancellationToken cancellationToken);

        /// <summary>The related objects of one state, as copy-on-write handles.</summary>
        public abstract RelatedSet ToRelatedSet(IReadOnlyList<object> items);

        /// <summary>The verbs the controller needs on the related resource.</summary>
        public virtual IReadOnlyList<string> Verbs => new[] { "get", "list", "watch" };
    }

    internal abstract class Relationship<T, TOther> : Relationship<T>
        where T : class, IKubernetesObject<V1ObjectMeta>
        where TOther : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly ITrigger<TOther> _trigger;

        protected Relationship(int index, RelationshipKind kind, string name, ITrigger<TOther> trigger) : base(index, kind, typeof(TOther), name) => _trigger = trigger;

        /// <summary>The other side's informer: scoped to the controller's namespace when namespaced, filtered by own writes and the trigger.</summary>
        protected async Task<IInformable<string, TOther>> SourceAsync(ControllerScope scope, CancellationToken cancellationToken)
        {
            var namespaced = (await scope.Client.GetResourceInfoAsync(GroupVersionResource.For<TOther>(), cancellationToken).ConfigureAwait(false)).Namespaced;
            IInformable<string, TOther> source = Scoped(scope.Informers.Get<TOther>(), namespaced ? scope.Namespace : null);
            if (scope.OwnWrites != null)
                source = source.IgnoreWritesFrom(scope.OwnWrites);
            if (_trigger != null)
                source = source.TriggerOn(_trigger);
            return source;
        }

        protected static async Task<bool> IsNamespacedAsync(ControllerScope scope, CancellationToken cancellationToken) =>
            (await scope.Client.GetResourceInfoAsync(GroupVersionResource.For<TOther>(), cancellationToken).ConfigureAwait(false)).Namespaced;

        private static IInformer<string, TOther> Scoped(IInformer<string, TOther> informer, string ns) =>
            ns == null ? informer : informer.Where(x => x.Metadata.NamespaceProperty == ns);

        public override RelatedSet ToRelatedSet(IReadOnlyList<object> items) => new RelatedSet<TOther>(Kind, Name, items.Cast<TOther>());
    }

    internal sealed class OwnsRelationship<T, TChild> : Relationship<T, TChild>
        where T : class, IKubernetesObject<V1ObjectMeta>
        where TChild : class, IKubernetesObject<V1ObjectMeta>
    {
        public OwnsRelationship(int index, ITrigger<TChild> trigger) : base(index, RelationshipKind.Owns, null, trigger)
        {
        }

        public override IReadOnlyList<string> Verbs => new[] { "get", "list", "watch", "create", "update", "patch", "delete" };

        public override async Task<IInformable<string, ControllerState<T>>> JoinAsync(IInformable<string, ControllerState<T>> state, ControllerScope scope, CancellationToken cancellationToken)
        {
            var kind = scope.PrimaryKind;
            var group = GroupOf(scope.PrimaryApiVersion);
            // observed before the join sees them, so expectations are met by the time a reconcile sees the children
            var children = scope.Expectations.Observe(await SourceAsync(scope, cancellationToken).ConfigureAwait(false),
                c => ControllerOwners(c, kind, group).Select(x => KubernetesObjectExtensions.ObjectKey(c.Metadata.NamespaceProperty, x.Name)));
            var index = Index;
            return state.GroupJoin(children, s => s.Primary.Metadata.Uid, c => ControllerOwners(c, kind, group).Select(x => x.Uid),
                (s, cs) => s.With(index, cs.ToList<object>()));
        }

        private static IEnumerable<V1OwnerReference> ControllerOwners(TChild child, string kind, string group) =>
            child.Metadata?.OwnerReferences?.Where(x => x.Controller == true && x.Uid != null && x.Kind == kind && GroupOf(x.ApiVersion) == group)
            ?? Enumerable.Empty<V1OwnerReference>();

        private static string GroupOf(string apiVersion)
        {
            var slash = apiVersion?.IndexOf('/') ?? -1;
            return slash < 0 ? string.Empty : apiVersion[..slash];
        }
    }

    internal sealed class ReferencesRelationship<T, TOther> : Relationship<T, TOther>
        where T : class, IKubernetesObject<V1ObjectMeta>
        where TOther : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly Func<T, IEnumerable<string>> _names;

        public ReferencesRelationship(int index, string name, Func<T, IEnumerable<string>> names, ITrigger<TOther> trigger) : base(index, RelationshipKind.References, name, trigger) =>
            _names = names;

        public override async Task<IInformable<string, ControllerState<T>>> JoinAsync(IInformable<string, ControllerState<T>> state, ControllerScope scope, CancellationToken cancellationToken)
        {
            var namespaced = await IsNamespacedAsync(scope, cancellationToken).ConfigureAwait(false);
            var others = await SourceAsync(scope, cancellationToken).ConfigureAwait(false);
            var names = _names;
            var index = Index;
            return state.GroupJoin(others,
                s => (names(s.Primary) ?? Enumerable.Empty<string>()).Where(x => !string.IsNullOrEmpty(x))
                    .Select(x => x.Contains('/') || !namespaced ? x : KubernetesObjectExtensions.ObjectKey(s.Primary.Metadata.NamespaceProperty, x)),
                o => new[] { o.ObjectKey() },
                (s, os) => s.With(index, os.ToList<object>()));
        }
    }

    internal sealed class WatchesRelationship<T, TOther> : Relationship<T, TOther>
        where T : class, IKubernetesObject<V1ObjectMeta>
        where TOther : class, IKubernetesObject<V1ObjectMeta>
    {
        private readonly Func<TOther, IEnumerable<string>> _primaryKeys;

        public WatchesRelationship(int index, string name, Func<TOther, IEnumerable<string>> primaryKeys, ITrigger<TOther> trigger) : base(index, RelationshipKind.Watches, name, trigger) =>
            _primaryKeys = primaryKeys;

        public override async Task<IInformable<string, ControllerState<T>>> JoinAsync(IInformable<string, ControllerState<T>> state, ControllerScope scope, CancellationToken cancellationToken)
        {
            var others = await SourceAsync(scope, cancellationToken).ConfigureAwait(false);
            var keys = _primaryKeys;
            var index = Index;
            return state.GroupJoin(others, s => new[] { s.Primary.ObjectKey() }, o => keys(o) ?? Enumerable.Empty<string>(), (s, os) => s.With(index, os.ToList<object>()));
        }
    }

    /// <summary>The related objects of one relationship in a reconcile context.</summary>
    public abstract class RelatedSet
    {
        private protected RelatedSet(RelationshipKind kind, string name, Type type)
        {
            Kind = kind;
            Name = name;
            Type = type;
        }

        public RelationshipKind Kind { get; }
        public string Name { get; }
        public Type Type { get; }

        internal abstract int UnsavedChanges();

        internal abstract Task SaveAsync(IKubernetesGenericClient client, CancellationToken cancellationToken);
    }

    internal sealed class RelatedSet<TOther> : RelatedSet where TOther : class, IKubernetesObject<V1ObjectMeta>
    {
        public RelatedSet(RelationshipKind kind, string name, IEnumerable<TOther> items) : base(kind, name, typeof(TOther)) =>
            Items = items.Select(x => new Tracked<TOther>(x)).ToList();

        public List<Tracked<TOther>> Items { get; }

        internal override int UnsavedChanges() => Items.Count(x => x.HasChanges());

        internal override async Task SaveAsync(IKubernetesGenericClient client, CancellationToken cancellationToken)
        {
            foreach (var item in Items.Where(x => x.IsModified))
                await client.SaveAsync(item, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }
}
