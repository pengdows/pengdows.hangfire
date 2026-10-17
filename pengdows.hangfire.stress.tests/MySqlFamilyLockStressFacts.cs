using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Hangfire.Storage;
using pengdows.hangfire.stress.tests.infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

public abstract class MySqlFamilyLockStressFacts<TFixture> where TFixture : BaseStressFixture
{
    private const int WorkerCount = 25;
    private readonly TFixture _fixture;
    private readonly ITestOutputHelper _output;

    protected MySqlFamilyLockStressFacts(TFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    protected abstract string ProviderName { get; }

    [Fact(Timeout = 180_000)]
    public async Task MutualExclusion_25ConcurrentWorkers_ZeroOverlap()
    {
        var resource = $"{ProviderName.ToLowerInvariant()}-stress-{Guid.NewGuid():N}";
        var tracker = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        var barrier = new Barrier(WorkerCount);
        long acquired = 0;
        long timeouts = 0;

        var workers = Enumerable.Range(0, WorkerCount).Select(_ =>
        {
            var thread = new Thread(() =>
            {
                barrier.SignalAndWait();
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    using var distributedLock = new PengdowsCrudDistributedLock(
                        _fixture.Storage, resource, TimeSpan.FromSeconds(120));
                    var owner = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, owner);
                    Interlocked.Increment(ref acquired);
                    Thread.Sleep(Random.Shared.Next(5, 15));
                    tracker.Exit(resource, owner, entered, DateTime.UtcNow);
                    latencies.Add(stopwatch.ElapsedMilliseconds);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }) { IsBackground = true };
            thread.Start();
            return thread;
        }).ToArray();

        await Task.Run(() =>
        {
            foreach (var worker in workers)
            {
                worker.Join();
            }
        });

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1);

        var sorted = latencies.OrderBy(value => value).ToList();
        _output.WriteLine(
            $"{ProviderName} Stress: workers={WorkerCount} acquired={acquired} " +
            $"timeouts={timeouts} violations={tracker.Violations} " +
            $"maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _output.WriteLine(
            $"Acquire-latency ms p50={Percentile(sorted, 50)} " +
            $"p95={Percentile(sorted, 95)} p99={Percentile(sorted, 99)} " +
            $"max={sorted.LastOrDefault()}");

        var monitoring = _fixture.Storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
        if (monitoring != null)
        {
            _output.WriteLine(monitoring.GetDatabaseMetricGrid());
        }
    }

    private static long Percentile(IReadOnlyList<long> sorted, int percentile)
    {
        if (sorted.Count == 0) return 0;
        var index = (int)(sorted.Count * (percentile / 100.0));
        return sorted[Math.Min(index, sorted.Count - 1)];
    }
}

[Collection("MySqlStress")]
public sealed class MySqlLockStressFacts : MySqlFamilyLockStressFacts<MySqlStressFixture>
{
    public MySqlLockStressFacts(MySqlStressFixture fixture, ITestOutputHelper output)
        : base(fixture, output) { }

    protected override string ProviderName => "MySQL";
}

[Collection("MariaDbStress")]
public sealed class MariaDbLockStressFacts : MySqlFamilyLockStressFacts<MariaDbStressFixture>
{
    public MariaDbLockStressFacts(MariaDbStressFixture fixture, ITestOutputHelper output)
        : base(fixture, output) { }

    protected override string ProviderName => "MariaDB";
}
