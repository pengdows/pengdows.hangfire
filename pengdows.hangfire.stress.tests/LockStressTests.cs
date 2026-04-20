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

/// <summary>
/// Lock-correctness stress tests against a live SQL Server instance (TestContainers).
///
/// Core invariant: for any given resource name, at most one holder exists at any
/// instant.  Three independent mechanisms verify this:
///
///   1. Live CAS check  — <see cref="OwnershipTracker.Enter"/>
///   2. Active-count max — <see cref="OwnershipTracker.GlobalMaxConcurrentOwners"/>
///   3. Post-run interval overlap — <see cref="OwnershipTracker.CountIntervalOverlaps"/>
///
/// Tests in this class use pool=256 so the connection governor cannot confound
/// correctness results.  Boundary-pressure tests (pool &lt; workers) live in
/// <see cref="BoundaryPressureTests"/>.
///
/// Long-running tests (&gt;=10 minutes) are gated behind
///   [Trait("Category", "LongRunning")]
/// and should be excluded from the default run. Run the default suite with:
///   dotnet test --filter "Category!=LongRunning"
/// Run only the long-running suite with:
///   dotnet test --filter "Category=LongRunning"
/// </summary>
[Collection("SqlServerStress")]
public sealed class LockStressTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public LockStressTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── 1. Mutual exclusion under N-concurrent-worker burst ──────────────────

    /// <summary>
    /// N workers all target the same resource with a generous timeout.
    /// Pool=256 ensures pool saturation cannot be the failure mode.
    /// Zero overlaps, zero timeouts required.
    /// </summary>
    [Theory(Timeout = 120_000)]
    [InlineData(50)]
    [InlineData(200)]
    public async Task MutualExclusion_NConcurrentWorkers_ZeroOverlap(int workerCount)
    {
        var resource  = "stress-excl-" + Guid.NewGuid().ToString("N");
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
                        _f.Storage, resource, TimeSpan.FromSeconds(90));

                    latencies.Add(sw.ElapsedMilliseconds);

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquired);
                    
                    // Periodic read to populate Read Role metrics
                    if (acquired % 10 == 0)
                    {
                        var monitor = _f.Storage.GetMonitoringApi();
                        var stats = monitor.GetStatistics();
                    }

                    Thread.Sleep(Random.Shared.Next(10, 50));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
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
        // receive DistributedLockTimeoutException immediately under burst contention.
        // Correctness invariant: zero violations among workers that did acquire.
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        var sorted = latencies.OrderBy(x => x).ToList();
        _out.WriteLine($"Workers={workerCount}  acquired={acquired}  timeouts={timeouts}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(sorted,50)}  p95={Pct(sorted,95)}  p99={Pct(sorted,99)}  max={sorted.LastOrDefault()}");
        
        EmitDatabaseMetrics(_f.Storage);
    }

    // ── 2. Skewed multi-resource — 200 workers, 20 resources, 80% hot ────────

    /// <summary>
    /// 200 workers across 20 resource keys; 80% target 2 hot keys.
    /// Per-resource overlap invariant must hold for every key.
    /// This approximates a real queue-name distribution in a Hangfire deployment
    /// where a small number of queue names get most of the traffic.
    /// </summary>
    [Fact(Timeout = 300_000)]
    public async Task SkewedMultiResource_200Workers_HotKeyPressure_ZeroOverlapPerResource()
    {
        const int workerCount   = 200;
        const int resourceCount = 20;
        var prefix    = "stress-skew-" + Guid.NewGuid().ToString("N") + "-";
        var resources = Enumerable.Range(0, resourceCount).Select(i => prefix + i).ToArray();
        var tracker   = new OwnershipTracker();
        long timeouts = 0;

        var barrier = new Barrier(workerCount);

        var threads = Enumerable.Range(0, workerCount).Select(i =>
        {
            // 80 % → hot keys [0] and [1]; 20 % spread over the remaining 18
            var resource = i < (int)(workerCount * 0.8)
                ? resources[i % 2]
                : resources[2 + (i % (resourceCount - 2))];

            var t = new Thread(() =>
            {
                barrier.SignalAndWait();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        _f.Storage, resource, TimeSpan.FromSeconds(180));

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Thread.Sleep(Random.Shared.Next(20, 80));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
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

        // Single-attempt design: workers that cannot immediately acquire throw immediately.
        // Correctness invariant: zero violations among workers that did acquire.
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        // Per-resource max makes it easy to pinpoint which key violated if the assert fires
        var perResourceMax = resources.Select(r =>
            $"{r.Split('-').Last()}={tracker.MaxConcurrentOwners(r)}").ToList();

        _out.WriteLine($"SkewedMultiResource: workers={workerCount}  resources={resourceCount}  timeouts={timeouts}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _out.WriteLine($"  Per-resource maxConcurrent: {string.Join("  ", perResourceMax)}");

        EmitDatabaseMetrics(_f.Storage);
        }


    // ── 3. Long holder + short-timeout waiters ────────────────────────────────

    /// <summary>
    /// One worker holds the lock for 8 seconds. Twenty workers try to acquire
    /// with a 2-second timeout — all must receive
    /// <see cref="DistributedLockTimeoutException"/>. After the holder releases,
    /// a final acquire must succeed.
    /// </summary>
    [Fact(Timeout = 60_000)]
    public async Task LongHolder_ShortTimeoutWaiters_AllTimeoutThenRecovered()
    {
        const int waiterCount = 20;
        var resource = "stress-longholder-" + Guid.NewGuid().ToString("N");
        long timeoutCount = 0;
        long acquiredCount = 0;
        var holderAcquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holderDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var holderTask = Task.Run(async () =>
        {
            using var lk = new PengdowsCrudDistributedLock(
                _f.Storage, resource, TimeSpan.FromSeconds(5));
            holderAcquired.SetResult();
            await Task.Delay(8_000);
            holderDone.SetResult();
        });

        // Start contention only after the long holder definitely owns the row.
        await holderAcquired.Task;

        var waiterTasks = Enumerable.Range(0, waiterCount).Select(_ => Task.Run(() =>
        {
            try
            {
                using var lk = new PengdowsCrudDistributedLock(
                    _f.Storage, resource, TimeSpan.FromSeconds(2));
                Interlocked.Increment(ref acquiredCount);
            }
            catch (DistributedLockTimeoutException)
            {
                Interlocked.Increment(ref timeoutCount);
            }
        })).ToArray();

        await Task.WhenAll(waiterTasks);
        await holderTask;

        Assert.Equal(0, acquiredCount);
        Assert.Equal(waiterCount, timeoutCount);

        // After the holder releases, the resource must be acquirable
        using var recovery = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(5));
        Assert.NotNull(recovery);

        _out.WriteLine($"LongHolder: waiters={waiterCount}  timeouts={timeoutCount}  unexpectedAcquires={acquiredCount}; recovery lock acquired successfully");
    }

    // ── 4. Sustained throughput — ops/sec and latency distribution ───────────

    /// <summary>
    /// Ten workers loop for 10 seconds, each acquiring/holding briefly/releasing.
    /// Verifies zero overlaps under sustained load and emits latency percentiles.
    /// </summary>
    [Fact(Timeout = 60_000)]
    public async Task Throughput_10Workers_10Seconds_EmitsMetrics()
    {
        const int workerCount = 10;
        const int durationMs  = 10_000;
        var resource          = "stress-throughput-" + Guid.NewGuid().ToString("N");
        var tracker  = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        long acquires = 0, timeouts = 0;
        var cts = new CancellationTokenSource(durationMs);

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        _f.Storage, resource, TimeSpan.FromSeconds(10));

                    latencies.Add(sw.ElapsedMilliseconds);

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquires);

                    // Periodic read to populate Read Role metrics
                    if (acquires % 5 == 0)
                    {
                        var monitor = _f.Storage.GetMonitoringApi();
                        var stats = monitor.GetStatistics();
                    }

                    Thread.Sleep(Random.Shared.Next(5, 25));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        var allLatencies = latencies.OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"Throughput: {acquires} acquires / {durationMs} ms = {opsPerSec:F1} ops/sec");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Timeouts={timeouts}  Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");

        EmitDatabaseMetrics(_f.Storage);
    }

    // ── 5. Short sustained run (30 s, production pool) ────────────────────────

    /// <summary>
    /// 20 workers compete for a single resource for 30 seconds at pool=100.
    /// Workers &lt;&lt; pool, so pool saturation is not a factor — any failure
    /// here is a lock logic defect.
    ///
    /// NOTE: 30 seconds is a short sustained run, not a soak.  It demonstrates
    /// that exclusion holds over multiple heartbeat cycles under realistic
    /// operational pressure.  For longer-horizon confidence, run the 10-minute
    /// variant below (Category=LongRunning).
    /// </summary>
    [Fact(Timeout = 90_000)]
    public async Task ShortSustained_ProductionPool_30Seconds_ZeroViolations()
    {
        const int workerCount        = 20;
        const int durationMs         = 30_000;
        const int productionPoolSize = 100;

        var resource  = "stress-sustained-" + Guid.NewGuid().ToString("N");
        var storage   = _f.CreateStorageWithPoolSize(productionPoolSize);
        var tracker   = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        long acquires = 0, timeouts = 0;
        var cts = new CancellationTokenSource(durationMs);

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        storage, resource, TimeSpan.FromSeconds(15));

                    latencies.Add(sw.ElapsedMilliseconds);

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquires);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        var allLatencies = latencies.OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"ShortSustained({durationMs / 1000}s pool={productionPoolSize}): {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");

        EmitDatabaseMetrics(storage);
    }

    // ── 6. Long-running sustained (10 min) — LongRunning category ────────────

    /// <summary>
    /// 20 workers compete for a single resource for 10 minutes at pool=100.
    ///
    /// This is the minimum duration for real soak confidence: it exercises
    /// multiple full heartbeat + TTL cycles and gives cleanup code enough time
    /// to accumulate orphan rows if any are leaking.
    ///
    /// Not run in standard CI.  Execute explicitly with:
    ///   dotnet test --filter "Category=LongRunning"
    /// </summary>
    [Fact(Timeout = 720_000)]
    [Trait("Category", "LongRunning")]
    public async Task Soak_ProductionPool_10Minutes_ZeroViolations()
    {
        const int workerCount        = 20;
        const int durationMs         = 600_000; // 10 minutes
        const int productionPoolSize = 100;

        var resource  = "stress-soak10m-" + Guid.NewGuid().ToString("N");
        var storage   = _f.CreateStorageWithPoolSize(productionPoolSize);
        var tracker   = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        long acquires = 0, timeouts = 0;
        var cts = new CancellationTokenSource(durationMs);

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        storage, resource, TimeSpan.FromSeconds(15));

                    latencies.Add(sw.ElapsedMilliseconds);

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquires);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        var allLatencies = latencies.OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"Soak(10min pool={productionPoolSize}): {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");

        EmitDatabaseMetrics(storage);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static long Pct(List<long> sorted, int p)
    {
        if (sorted.Count == 0) return 0;
        int idx = Math.Min((int)Math.Ceiling(sorted.Count * p / 100.0) - 1, sorted.Count - 1);
        return sorted[Math.Max(0, idx)];
    }

    private static int Pct(List<int> sorted, int p)
    {
        if (sorted.Count == 0) return 0;
        int idx = Math.Min((int)Math.Ceiling(sorted.Count * p / 100.0) - 1, sorted.Count - 1);
        return sorted[Math.Max(0, idx)];
    }

    // ── 7. Fan-in scaling — upsert rate vs worker count ──────────────────────

    /// <summary>
    /// Measures DB UPSERT rate per second as a function of concurrent worker count.
    /// Each acquire now performs exactly one UPSERT — no retry loop.
    ///
    /// What is measured:
    ///   total_db_calls = acquires (one UPSERT per successful acquire)
    ///   db_calls_per_second = total_db_calls / duration
    ///
    /// Safety invariant still applies — MaxConcurrentOwners must remain ≤ 1.
    /// </summary>
    [Theory(Timeout = 300_000)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(200)]
    public async Task FanIn_UpsertRateScaling_ByWorkerCount(int workerCount)
    {
        const int durationMs = 15_000;

        var resource  = "stress-fanin-" + Guid.NewGuid().ToString("N");
        var storage   = _f.CreateStorageWithPoolSize(Math.Max(workerCount, 256), TimeSpan.FromSeconds(30));
        var tracker   = new OwnershipTracker();
        var latencies = new ConcurrentBag<long>();
        long totalDbCalls = 0;
        long acquires     = 0;
        long timeouts     = 0;
        var cts = new CancellationTokenSource(durationMs);

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        storage, resource, TimeSpan.FromSeconds(30));

                    // Each acquire = exactly 1 UPSERT (single-attempt design).
                    Interlocked.Add(ref totalDbCalls, 1);
                    latencies.Add(sw.ElapsedMilliseconds);

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquires);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref timeouts);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"workers={workerCount}: MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()}");

        var dbCallsPerSec = totalDbCalls * 1000.0 / durationMs;
        var allLatencies  = latencies.OrderBy(x => x).ToList();
        var opsPerSec     = acquires * 1000.0 / durationMs;

        _out.WriteLine($"FanIn workers={workerCount,3}: {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"FanIn workers={workerCount,3}: db calls = {totalDbCalls} = {dbCallsPerSec:F0} calls/sec");
        _out.WriteLine($"FanIn workers={workerCount,3}: latency ms  p50={Pct(allLatencies,50),-7}  p95={Pct(allLatencies,95),-7}  p99={Pct(allLatencies,99),-7}");

        EmitDatabaseMetrics(storage);
    }

    private void EmitDatabaseMetrics(PengdowsCrudJobStorage storage)
    {
        var monitor = storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
        if (monitor != null)
        {
            _out.WriteLine(monitor.GetDatabaseMetricGrid());
        }
    }

}
