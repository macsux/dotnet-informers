using System.Linq.Expressions;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Translates single terms of a <c>Where</c> predicate into server filter terms. The base library splits a predicate into its
    ///     top-level <c>&amp;&amp;</c> terms and offers each one to the translator; terms it can't translate are evaluated client-side.
    /// </summary>
    /// <typeparam name="TResource">The type of resource the predicate is on</typeparam>
    public interface IFilterTranslator<TResource>
    {
        /// <summary>
        ///     Tries to translate one term. A translated term is dropped from client-side evaluation, so the server filter it adds must accept
        ///     exactly the objects the term accepts. When unsure, return <see langword="false" />.
        /// </summary>
        /// <param name="term">The term, a boolean expression over <paramref name="parameter" /></param>
        /// <param name="parameter">The predicate's parameter</param>
        /// <param name="current">The server filter built from the terms translated so far</param>
        /// <param name="result">The server filter with this term added</param>
        bool TryTranslate(Expression term, ParameterExpression parameter, ServerFilter current, out ServerFilter result);
    }
}
