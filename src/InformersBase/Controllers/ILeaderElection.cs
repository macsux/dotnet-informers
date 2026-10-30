using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase
{
    /// <summary>
    ///     Gates <c>RunAsync</c> handlers on leadership, so only one replica of a controller acts at a time while every replica keeps its
    ///     informers (and caches) warm.
    /// </summary>
    public interface ILeaderElection
    {
        /// <summary>
        ///     Completes once this instance is the leader, with a token that's cancelled the moment leadership is lost.
        /// </summary>
        Task<CancellationToken> WaitForLeadershipAsync(CancellationToken cancellationToken);
    }
}
