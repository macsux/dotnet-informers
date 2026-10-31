using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Decides whether a modification of an object is worth a reconcile (see <c>TriggerOn</c>). A trigger captures what it compares from each
    ///     version of an object; the capture is kept per key, and the next version's capture is compared with it.
    /// </summary>
    /// <typeparam name="T">The type of object, or a base type of it</typeparam>
    public interface ITrigger<in T>
    {
        /// <summary>What to remember of <paramref name="value" /> for the next comparison (kept per key, so keep it small).</summary>
        object Capture(T value);

        /// <summary>The name of what changed between two captures (e.g. <c>spec</c>), or <see langword="null" /> when nothing did.</summary>
        string Changed(object previous, object current);
    }

    /// <summary>Building blocks for <see cref="ITrigger{T}" />.</summary>
    public partial class Trigger
    {
        private protected Trigger()
        {
        }

        /// <summary>Every change triggers (no filtering).</summary>
        public static ITrigger<object> Any { get; } = new AnyTrigger();

        /// <summary>
        ///     Triggers when one of the selected values changes. Values compare structurally: primitives and strings by value, collections and
        ///     dictionaries element by element, other objects by their JSON (through <see cref="InformerJson" />). The selectors' member paths name
        ///     what changed (<c>x =&gt; x.Metadata.Labels</c> is <c>metadata.labels</c>).
        /// </summary>
        public static ITrigger<T> On<T>(params Expression<Func<T, object>>[] selectors)
        {
            if (selectors == null || selectors.Length == 0)
                throw new ArgumentException("At least one selector is needed", nameof(selectors));
            return selectors.Length == 1 ? new FieldTrigger<T>(selectors[0]) : Combine(selectors.Select(x => (ITrigger<T>)new FieldTrigger<T>(x)).ToArray());
        }

        /// <summary>Triggers when one of the selected values changes, named explicitly (for values that aren't member paths).</summary>
        public static ITrigger<T> On<T>(string name, Func<T, object> selector, IEqualityComparer<object> comparer = null) =>
            new FieldTrigger<T>(name ?? throw new ArgumentNullException(nameof(name)), selector ?? throw new ArgumentNullException(nameof(selector)), comparer);

        /// <summary>
        ///     Triggers when <paramref name="changed" /> returns true for the previous and current object, for rules a projection can't express.
        ///     It keeps the whole previous object per key.
        /// </summary>
        public static ITrigger<T> When<T>(Func<T, T, bool> changed, string name = "custom") => new PredicateTrigger<T>(changed ?? throw new ArgumentNullException(nameof(changed)), name);

        /// <summary>Triggers when any of <paramref name="triggers" /> does.</summary>
        public static ITrigger<T> Combine<T>(params ITrigger<T>[] triggers)
        {
            if (triggers == null || triggers.Length == 0)
                throw new ArgumentException("At least one trigger is needed", nameof(triggers));
            return triggers.Length == 1 ? triggers[0] : new CompositeTrigger<T>(triggers);
        }

        private sealed class AnyTrigger : ITrigger<object>
        {
            public object Capture(object value) => null;
            public string Changed(object previous, object current) => "any";
        }

        private sealed class PredicateTrigger<T> : ITrigger<T>
        {
            private readonly Func<T, T, bool> _changed;
            private readonly string _name;

            public PredicateTrigger(Func<T, T, bool> changed, string name)
            {
                _changed = changed;
                _name = name;
            }

            public object Capture(T value) => value;
            public string Changed(object previous, object current) => _changed((T)previous, (T)current) ? _name : null;
        }

        private sealed class CompositeTrigger<T> : ITrigger<T>
        {
            private readonly ITrigger<T>[] _triggers;

            public CompositeTrigger(ITrigger<T>[] triggers) => _triggers = triggers;

            public object Capture(T value)
            {
                var captures = new object[_triggers.Length];
                for (var i = 0; i < _triggers.Length; i++)
                    captures[i] = _triggers[i].Capture(value);
                return captures;
            }

            public string Changed(object previous, object current)
            {
                var before = (object[])previous;
                var after = (object[])current;
                List<string> changed = null;
                for (var i = 0; i < _triggers.Length; i++)
                {
                    var name = _triggers[i].Changed(before[i], after[i]);
                    if (name != null)
                        (changed ??= new List<string>()).Add(name);
                }

                return changed == null ? null : string.Join(", ", changed);
            }
        }

        private sealed class FieldTrigger<T> : ITrigger<T>
        {
            private readonly string _name;
            private readonly Func<T, object> _selector;
            private readonly IEqualityComparer<object> _comparer;

            public FieldTrigger(Expression<Func<T, object>> selector)
                : this(NameOf(selector ?? throw new ArgumentNullException(nameof(selector))), Compile(selector), null)
            {
            }

            public FieldTrigger(string name, Func<T, object> selector, IEqualityComparer<object> comparer)
            {
                _name = name;
                _selector = selector;
                _comparer = comparer;
            }

            public object Capture(T value)
            {
                object selected;
                try
                {
                    selected = value == null ? null : _selector(value);
                }
                catch (NullReferenceException)
                {
                    selected = null; // x => x.Status.Phase on an object without status
                }

                return _comparer != null ? new Box(selected) : StructuralValue.Capture(selected);
            }

            public string Changed(object previous, object current)
            {
                var equal = _comparer != null ? _comparer.Equals(((Box)previous).Value, ((Box)current).Value) : Equals(previous, current);
                return equal ? null : _name;
            }

            private sealed record Box(object Value);

            private static Func<T, object> Compile(Expression<Func<T, object>> selector) =>
                selector.Compile(preferInterpretation: !RuntimeFeature.IsDynamicCodeSupported);

            /// <summary><c>x =&gt; x.Metadata.Labels</c> is <c>metadata.labels</c>; anything else is the expression's text.</summary>
            internal static string NameOf(LambdaExpression selector)
            {
                var body = selector.Body;
                while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
                    body = convert.Operand;
                var parts = new List<string>();
                while (body is MemberExpression member)
                {
                    parts.Add(member.Member.Name);
                    body = member.Expression;
                }

                if (body != selector.Parameters[0] || parts.Count == 0)
                    return selector.Body.ToString();
                parts.Reverse();
                var sb = new StringBuilder();
                foreach (var part in parts)
                {
                    if (sb.Length > 0)
                        sb.Append('.');
                    sb.Append(char.ToLowerInvariant(part[0])).Append(part, 1, part.Length - 1);
                }

                return sb.ToString();
            }
        }
    }
}
