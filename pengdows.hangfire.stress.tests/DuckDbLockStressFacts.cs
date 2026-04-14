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

[Collection("DuckDbStress")]
public class DuckDbLockStressFacts
{
    private readonly DuckDbStressFixture _f;
    private readonly ITestOutputHelper _out;

    public DuckDbLockStressFacts(DuckDbStressFixture fixture, ITestOutputHelper output)
    {
        _f = fixture;
        _out = output;
    }

    [Fact(Timeout = 30_000)]
    public async Task MutualExclusion_20ConcurrentWorkers_ZeroOverlap_DuckDbSingleWriter()
    {
        // DuckDB is a single-process embedded database — realistic deployments have one
        // worker process with a handful of concurrent Hangfire threads, not hundreds.
        const int workerCount = 20;
        var resource  = "duckdb-stress-" + Guid.NewGuid().ToString("N");
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
                    
                    // Simulate work
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

        // Single-attempt design: workers that cannot immediately steal an expired row
        // receive DistributedLockTimeoutException immediately — timeouts are expected
        // under burst contention.  Correctness invariant: zero violations.
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1);

        var sorted = latencies.OrderBy(x => x).ToList();
        _out.WriteLine($"DuckDB SingleWriter Stress (single-process): workers={workerCount}  acquired={acquired}  timeouts={timeouts}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
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
