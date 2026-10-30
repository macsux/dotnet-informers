using System;
using System.Collections.Generic;
using System.Linq;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     One term of a <see cref="ServerFilter" />. The base library treats terms as opaque: <see cref="Kind" /> and <see cref="Value" />
    ///     are defined by the source's <see cref="IFilterTranslator{TResource}" /> and interpreted by its producer
    ///     (e.g. Kubernetes uses kinds <c>namespace</c>, <c>label</c> and <c>field</c>).
    /// </summary>
    public readonly struct ServerFilterTerm : IEquatable<ServerFilterTerm>, IComparable<ServerFilterTerm>
    {
        public ServerFilterTerm(string kind, string value)
        {
            Kind = kind ?? throw new ArgumentNullException(nameof(kind));
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        public string Kind { get; }
        public string Value { get; }

        public bool Equals(ServerFilterTerm other) => string.Equals(Kind, other.Kind, StringComparison.Ordinal) && string.Equals(Value, other.Value, StringComparison.Ordinal);
        public override bool Equals(object obj) => obj is ServerFilterTerm other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Kind, Value);

        public int CompareTo(ServerFilterTerm other)
        {
            var result = string.CompareOrdinal(Kind, other.Kind);
            return result != 0 ? result : string.CompareOrdinal(Value, other.Value);
        }

        public override string ToString() => $"{Kind}:{Value}";
    }

    /// <summary>
    ///     A canonical set of filter terms pushed down to the source's server subscription. Terms are kept sorted and distinct,
    ///     so two equivalent predicates produce equal filters and share one producer.
    /// </summary>
    public sealed class ServerFilter : IEquatable<ServerFilter>
    {
        private readonly ServerFilterTerm[] _terms;
        private readonly int _hashCode;

        public static ServerFilter Empty { get; } = new ServerFilter(Array.Empty<ServerFilterTerm>());

        private ServerFilter(ServerFilterTerm[] terms)
        {
            _terms = terms;
            var hash = new HashCode();
            foreach (var term in terms)
                hash.Add(term);
            _hashCode = hash.ToHashCode();
        }

        public static ServerFilter Create(IEnumerable<ServerFilterTerm> terms)
        {
            var sorted = terms.Distinct().OrderBy(x => x).ToArray();
            return sorted.Length == 0 ? Empty : new ServerFilter(sorted);
        }

        /// <summary>The terms, sorted.</summary>
        public IReadOnlyList<ServerFilterTerm> Terms => _terms;

        public bool IsEmpty => _terms.Length == 0;

        /// <summary>Returns a filter with the given term added.</summary>
        public ServerFilter With(ServerFilterTerm term) => Create(_terms.Append(term));

        /// <summary>Returns a filter with the given terms added.</summary>
        public ServerFilter With(IEnumerable<ServerFilterTerm> terms) => Create(_terms.Concat(terms));

        /// <summary>The values of every term of the given kind.</summary>
        public IEnumerable<string> ValuesOf(string kind) => _terms.Where(x => x.Kind == kind).Select(x => x.Value);

        public bool Equals(ServerFilter other) => other != null && _hashCode == other._hashCode && _terms.AsSpan().SequenceEqual(other._terms);
        public override bool Equals(object obj) => Equals(obj as ServerFilter);
        public override int GetHashCode() => _hashCode;
        public override string ToString() => IsEmpty ? "(none)" : string.Join(", ", _terms);
    }
}
