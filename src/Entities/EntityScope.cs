using System;

namespace Steeltoe.Informers.Entities
{
    /// <summary>Whether a custom resource is namespaced or cluster-scoped.</summary>
    public enum EntityScope
    {
        Namespaced,
        Cluster,
    }

    /// <summary>The scope of the custom resource (default: namespaced).</summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public class EntityScopeAttribute(EntityScope scope = default) : Attribute
    {
        public EntityScope Scope => scope;
    }
}
