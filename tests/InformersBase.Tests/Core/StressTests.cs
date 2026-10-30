using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests.Core
{
    public class StressTests
    {
        private readonly ITestOutputHelper _output;

        public StressTests(ITestOutputHelper output) => _output = output;

        private static int Iterations(int fallback) =>
            int.TryParse(Environment.GetEnvironmentVariable("INFORMER_STRESS_ITERATIONS"), out var n) ? n : fallback;

        [Fact]
        public async Task RandomJoinLeave_ViewsAlwaysEqualCache()
        {
            var iterations = Iterations(40);
            var total = new StressResult();
            for (var seed = 1; seed <= iterations; seed++)
            {
                var r = await StressHarness.RunAsync(new StressConfig { Seed = seed });
                Accumulate(total, r);
                Assert.True(r.Failures.IsEmpty, $"seed {seed}: {r}\n{string.Join("\n", r.Failures.Take(20))}");
            }

            _output.WriteLine($"iterations={iterations} {total}");
        }

        [Fact]
        public async Task JoinStorm_WhileProducingFlatOut()
        {
            // Many consumers that read only a few hundred events each, so joins happen constantly while the producer runs.
            var iterations = Iterations(10);
            var total = new StressResult();
            for (var seed = 1; seed <= iterations; seed++)
            {
                var r = await StressHarness.RunAsync(new StressConfig
                {
                    Seed = 10_000 + seed, Keys = 50, Operations = 50_000, MaxConsumers = 32, CheckpointEvery = 1_000, RelistProbability = 0.001,
                });
                Accumulate(total, r);
                Assert.True(r.Failures.IsEmpty, $"seed {seed}: {r}\n{string.Join("\n", r.Failures.Take(20))}");
            }

            _output.WriteLine($"iterations={iterations} {total}");
        }

        // FaultMode is internal, so the theory takes its name
        [Theory]
        [InlineData(nameof(FaultMode.MarkAfterApplySeparateLock), false)]
        [InlineData(nameof(FaultMode.MarkBeforeApply), true)]
        [InlineData(nameof(FaultMode.NonAtomicRegistration), true)]
        [InlineData(nameof(FaultMode.PerKeyRevision), true)]
        public async Task Mutations_AreDetectedOrHarmless(string faultName, bool expectFailure)
        {
            var fault = Enum.Parse<FaultMode>(faultName);
            var iterations = Iterations(40);
            var failedRuns = 0;
            string firstFailure = null;
            var total = new StressResult();
            for (var seed = 1; seed <= iterations; seed++)
            {
                var r = await StressHarness.RunAsync(new StressConfig
                {
                    Seed = seed, Fault = fault, Keys = 30, DeleteProbability = 0.45, CheckpointEvery = 1_000,
                });
                Accumulate(total, r);
                if (!r.Failures.IsEmpty)
                {
                    failedRuns++;
                    firstFailure ??= $"seed {seed}: {r.Failures.First()}";
                }
            }

            _output.WriteLine($"{fault}: {failedRuns}/{iterations} runs failed; first: {firstFailure}; {total}");
            if (expectFailure)
                Assert.True(failedRuns > 0, $"{fault} was never detected in {iterations} runs");
            else
                Assert.True(failedRuns == 0, $"{fault} failed {failedRuns}/{iterations}: {firstFailure}");
        }

        internal static void Accumulate(StressResult total, StressResult r)
        {
            total.Joins += r.Joins;
            total.Leaves += r.Leaves;
            total.LeftMidReset += r.LeftMidReset;
            total.JoinedBeforeSync += r.JoinedBeforeSync;
            total.Relists += r.Relists;
            total.Checkpoints += r.Checkpoints;
            total.ViewsVerified += r.ViewsVerified;
            total.EventsDelivered += r.EventsDelivered;
            total.ResetsDelivered += r.ResetsDelivered;
            total.ConsumersCompleted += r.ConsumersCompleted;
            total.Elapsed += r.Elapsed;
        }
    }
}
