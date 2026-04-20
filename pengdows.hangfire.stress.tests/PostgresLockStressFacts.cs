using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

[Collection("PostgresStress")]
public class PostgresLockStressFacts
{
    private readonly PostgresStressFixture _f;
    private readonly ITestOutputHelper _out;

    public PostgresLockStressFacts(PostgresStressFixture fixture, ITestOutputHelper output)
    {
        _f = fixture;
        _out = output;
    }

    [Fact(Timeout = 120_000)]
    public async Task MutualExclusion_200ConcurrentWorkers_ZeroOverlap_Postgres()
    {
        const int workerCount = 200;
        var resource  = "postgres-stress-" + Guid.NewGuid().ToString("N");
        var tracker   = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        long acquired = 0, timeouts = 0;

        var barrier = new Barrier(workerCount);

        var threads = Enumerable.Range(0, workerCount).Select(_ =>
        {
            var t = new Thread(() =>
            {
                barrier.SignalAndWait();
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        _f.Storage, resource, TimeSpan.FromSeconds(120));

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquired);

                    Thread.Sleep(Random.Shared.Next(5, 15));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                    latencies.Add(sw.ElapsedMilliseconds);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }) { IsBackground = true };
            t.Start();
            return t;
        }).ToArray();

        await Task.Run(() => { foreach (var t in threads) t.Join(); });

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1);

        var sorted = latencies.OrderBy(x => x).ToList();
        _out.WriteLine($"PostgreSQL Stress: workers={workerCount}  acquired={acquired}  timeouts={timeouts}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(sorted,50)}  p95={Pct(sorted,95)}  p99={Pct(sorted,99)}  max={sorted.LastOrDefault()}");

        EmitDatabaseMetrics(_f.Storage);
    }

    private void EmitDatabaseMetrics(PengdowsCrudJobStorage storage)
    {
        var monitor = storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
        if (monitor != null)
        {
            _out.WriteLine(monitor.GetDatabaseMetricGrid());
        }
    }

    private static long Pct(List<long> sorted, int pct)
    {
        if (sorted.Count == 0) return 0;
        var idx = (int)(sorted.Count * (pct / 100.0));
        return sorted[Math.Min(idx, sorted.Count - 1)];
    }
}
