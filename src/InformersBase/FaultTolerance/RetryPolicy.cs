using System;
using System.Threading;
using System.Threading.Tasks;

namespace Steeltoe.Informers.InformersBase.FaultTolerance
{
    /// <summary>
    ///     Specifies which errors are retried and how long to wait before each attempt
    /// </summary>
    public class RetryPolicy
    {
        /// <summary>
        ///     No retry policy should be applied
        /// </summary>
        public static readonly RetryPolicy None = new RetryPolicy((_, __) => false, _ => TimeSpan.Zero);

        /// <param name="shouldRetry">A delegate which accepts exception being handled and retry attempt, and returns if retry should be attempted </param>
        /// <param name="retryDelay">A delegate that accepts retry attempt and returns delay till next retry attempt</param>
        public RetryPolicy(Func<Exception, int, bool> shouldRetry, Func<int, TimeSpan> retryDelay)
        {
            ShouldRetry = shouldRetry;
            RetryDelay = retryDelay;
        }

        internal Func<Exception, int, bool> ShouldRetry { get; }
        internal Func<int, TimeSpan> RetryDelay { get; }

        /// <summary>
        ///     Executes a given task while applying the specified retry policy
        /// </summary>
        /// <param name="action">Delegate for the task to execute</param>
        /// <param name="cancellationToken">Cancels the waits between attempts</param>
        /// <typeparam name="TResult">Return type of the Task</typeparam>
        /// <returns>Task result</returns>
        public async Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> action, CancellationToken cancellationToken = default)
        {
            var retryCount = 1;
            while (true)
            {
                try
                {
                    return await action().ConfigureAwait(false);
                }
                catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
                {
                    if (!ShouldRetry(e, retryCount))
                    {
                        throw;
                    }

                    retryCount++;
                    await Task.Delay(RetryDelay(retryCount), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
