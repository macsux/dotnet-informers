using System;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>Thrown from a consumer's <c>MoveNextAsync</c> when the informer's producer failed; the cause is the inner exception.</summary>
    public class InformerException : Exception
    {
        public InformerException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
