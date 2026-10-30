using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Splits a predicate into a server filter and a client-side remainder: only top-level <c>&amp;&amp;</c> terms are offered to the
    ///     translator, so an <c>||</c> that mixes translatable and untranslatable terms stays client-side as a whole.
    /// </summary>
    internal static class FilterSplitter
    {
        public static (ServerFilter Filter, Expression<Func<TResource, bool>> Remainder) Split<TResource>(
            Expression<Func<TResource, bool>> predicate,
            IFilterTranslator<TResource> translator,
            ServerFilter current)
        {
            if (translator == null)
                return (current, predicate);

            var parameter = predicate.Parameters[0];
            var remaining = new List<Expression>();
            var filter = current;
            foreach (var term in Flatten(predicate.Body))
            {
                if (TryTranslate(translator, term, parameter, filter, out var translated))
                    filter = translated;
                else
                    remaining.Add(term);
            }

            if (remaining.Count == 0)
                return (filter, null);
            var body = remaining.Aggregate(Expression.AndAlso);
            return (filter, Expression.Lambda<Func<TResource, bool>>(body, parameter));
        }

        private static bool TryTranslate<TResource>(IFilterTranslator<TResource> translator, Expression term, ParameterExpression parameter, ServerFilter current, out ServerFilter result)
        {
            try
            {
                if (translator.TryTranslate(term, parameter, current, out result) && result != null)
                    return true;
            }
            catch (Exception)
            {
                // a translator that can't make sense of a term leaves it client-side
            }

            result = current;
            return false;
        }

        internal static IEnumerable<Expression> Flatten(Expression body)
        {
            if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } and)
                return Flatten(and.Left).Concat(Flatten(and.Right));
            return new[] { body };
        }

        /// <summary>Combines two client-side predicates; either may be null.</summary>
        public static Func<TResource, bool> And<TResource>(Func<TResource, bool> left, Func<TResource, bool> right)
        {
            if (left == null)
                return right;
            if (right == null)
                return left;
            return x => left(x) && right(x);
        }
    }
}
