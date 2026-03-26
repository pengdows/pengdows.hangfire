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
/// and excluded from the default CI run.  Run them with:
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

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.Equal(0, timeouts); // 90s timeout — no worker should time out
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

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.Equal(0, timeouts); // 180s timeout — no worker should time out
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
        var holderDone = new TaskCompletionSource();

        var holderTask = Task.Run(async () =>
        {
            using var lk = new PengdowsCrudDistributedLock(
                _f.Storage, resource, TimeSpan.FromSeconds(5));
            await Task.Delay(8_000);
            holderDone.SetResult();
        });

        // Give the holder time to acquire before launching waiters
        await Task.Delay(300);

        var waiterTasks = Enumerable.Range(0, waiterCount).Select(_ => Task.Run(() =>
        {
            try
            {
                using var lk = new PengdowsCrudDistributedLock(
                    _f.Storage, resource, TimeSpan.FromSeconds(2));
            }
            catch (DistributedLockTimeoutException)
            {
                Interlocked.Increment(ref timeoutCount);
            }
        })).ToArray();

        await Task.WhenAll(waiterTasks);
        await holderTask;

        Assert.Equal(waiterCount, timeoutCount);

        // After the holder releases, the resource must be acquirable
        using var recovery = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(5));
        Assert.NotNull(recovery);

        _out.WriteLine($"LongHolder: {waiterCount} waiters all timed out; recovery lock acquired successfully");
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
        var tracker           = new OwnershipTracker();
        var modeLatencies     = ModeLatencyBags();
        var followRetryCounts = new ConcurrentBag<int>();
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

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);
                    if (lk.HowAcquired == AcquireMode.FollowRelease)
                    {
                        followRetryCounts.Add(lk.AcquireRetryCount);
                    }

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

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"Throughput: {acquires} acquires / {durationMs} ms = {opsPerSec:F1} ops/sec");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Timeouts={timeouts}  Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        
        EmitDatabaseMetrics(_f.Storage);
        EmitModeBreakdown(modeLatencies, followRetryCounts);
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

        var resource          = "stress-sustained-" + Guid.NewGuid().ToString("N");
        var storage           = _f.CreateStorageWithPoolSize(productionPoolSize);
        var tracker           = new OwnershipTracker();
        var modeLatencies     = ModeLatencyBags();
        var followRetryCounts = new ConcurrentBag<int>();
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

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);
                    if (lk.HowAcquired == AcquireMode.FollowRelease)
                    {
                        followRetryCounts.Add(lk.AcquireRetryCount);
                    }

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

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"ShortSustained({durationMs / 1000}s pool={productionPoolSize}): {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        
        EmitDatabaseMetrics(storage);
        EmitModeBreakdown(modeLatencies, followRetryCounts);
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

        var resource      = "stress-soak10m-" + Guid.NewGuid().ToString("N");
        var storage       = _f.CreateStorageWithPoolSize(productionPoolSize);
        var tracker       = new OwnershipTracker();
        var modeLatencies = ModeLatencyBags();
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

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);

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

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"Soak(10min pool={productionPoolSize}): {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"Acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"Violations={tracker.Violations}  IntervalOverlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        
        EmitDatabaseMetrics(storage);
        EmitModeBreakdown(modeLatencies);
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

    // ── 7. Wait-strategy sweep — retry delay vs latency/throughput ───────────

    /// <summary>
    /// Runs a fixed-duration sustained load scenario at each retry delay value
    /// and reports latency percentiles, acquire-mode breakdown, and throughput
    /// side-by-side.
    ///
    /// This quantifies whether the p95/p99 tail is inherent to the lease model
    /// or an artefact of the retry-sleep granularity.
    ///
    /// Shorter delays reduce latency quantization but increase DB call volume;
    /// longer delays reduce traffic but coarsen waiter wake-up timing.
    /// </summary>
    [Theory(Timeout = 120_000)]
    [InlineData(25)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(200)]
    public async Task WaitStrategy_RetryDelay_LatencyAndThroughput(int retryDelayMs)
    {
        const int workerCount = 10;
        const int durationMs  = 15_000;

        var resource      = "stress-waitstrat-" + Guid.NewGuid().ToString("N");
        var storage       = _f.CreateStorageWithTtl(
            retryDelayMs: retryDelayMs,
            ttl:          TimeSpan.FromSeconds(30));
        var tracker       = new OwnershipTracker();
        var modeLatencies = ModeLatencyBags();
        var followRetryCounts = new ConcurrentBag<int>();
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
                        storage, resource, TimeSpan.FromSeconds(30));

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);
                    if (lk.HowAcquired == AcquireMode.FollowRelease)
                    {
                        followRetryCounts.Add(lk.AcquireRetryCount);
                    }

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
            $"retryDelay={retryDelayMs}ms: MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()}");

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;

        _out.WriteLine($"[delay={retryDelayMs,3}ms] {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"[delay={retryDelayMs,3}ms] latency ms  p50={Pct(allLatencies,50),-7}  p95={Pct(allLatencies,95),-7}  p99={Pct(allLatencies,99),-7}  max={allLatencies.LastOrDefault()}");
        
        EmitDatabaseMetrics(storage);
        EmitModeBreakdown(modeLatencies, followRetryCounts);
    }

    // ── 8. Fixed vs jittered delay — phase alignment effect ──────────────────

    /// <summary>
    /// Runs the same sustained scenario twice at 50 ms — once with the fixed
    /// delay and once with jitter — to isolate the herd-collision effect.
    ///
    /// With fixed delay, all waiters sleep the same interval and wake together,
    /// creating synchronized collision spikes after a release.
    /// With jitter ([25 ms, 75 ms]), wake-ups are staggered and the release
    /// edge is consumed by one waiter while others are still sleeping.
    ///
    /// Observable difference: jitter should reduce p95/p99 for
    /// <see cref="AcquireMode.FollowRelease"/> without meaningfully changing
    /// throughput or <see cref="AcquireMode.InsertWin"/> latency.
    /// </summary>
    [Theory(Timeout = 120_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitStrategy_Jitter_vs_Fixed_LatencyProfile(bool jitter)
    {
        const int workerCount = 10;
        const int durationMs  = 20_000;
        const int delayMs     = 50;

        var resource          = "stress-jitter-" + Guid.NewGuid().ToString("N");
        var storage           = _f.CreateStorageWithTtl(delayMs, TimeSpan.FromSeconds(30), jitter);
        var tracker           = new OwnershipTracker();
        var modeLatencies     = ModeLatencyBags();
        var followRetryCounts = new ConcurrentBag<int>();
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
                        storage, resource, TimeSpan.FromSeconds(30));

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);
                    if (lk.HowAcquired == AcquireMode.FollowRelease)
                    {
                        followRetryCounts.Add(lk.AcquireRetryCount);
                    }

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
            $"jitter={jitter}: MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()}");

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;

        var label = jitter ? "jitter" : "fixed ";
        _out.WriteLine($"[{label}] {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"[{label}] latency ms  p50={Pct(allLatencies,50),-7}  p95={Pct(allLatencies,95),-7}  p99={Pct(allLatencies,99),-7}  max={allLatencies.LastOrDefault()}");
        
        EmitDatabaseMetrics(storage);
        EmitModeBreakdown(modeLatencies, followRetryCounts);
    }

    // ── 9. Fan-in scaling — poll rate vs worker count ─────────────────────────

    /// <summary>
    /// Measures DB poll rate (TryAcquire + TryDeleteExpired calls per second)
    /// as a function of concurrent worker count.
    ///
    /// Poll rate = workers × (hold_time / delay + 1) × (TryAcquire + TryDelete per retry).
    /// At 25ms delay and 200 workers this approaches thousands of calls/second
    /// on the lock table — a potential read storm at scale.
    ///
    /// What is measured:
    ///   total_db_calls ≈ Σ(2 × retryCount + 1) over all acquires
    ///   db_calls_per_second = total_db_calls / duration
    ///
    /// Safety invariant still applies — MaxConcurrentOwners must remain ≤ 1.
    /// </summary>
    [Theory(Timeout = 300_000)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(200)]
    public async Task FanIn_PollRateScaling_ByWorkerCount(int workerCount)
    {
        const int durationMs = 15_000;
        const int delayMs    = 50;

        var resource          = "stress-fanin-" + Guid.NewGuid().ToString("N");
        var storage           = _f.CreateStorageWithTtl(delayMs, TimeSpan.FromSeconds(30));
        var tracker           = new OwnershipTracker();
        var modeLatencies     = ModeLatencyBags();
        var followRetryCounts = new ConcurrentBag<int>();
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

                    // Each acquire: per sleep retry = 1 INSERT fail + 1 UPDATE miss = 2 calls.
                    // Final successful step: INSERT win = 1 call; steal (INSERT fail + UPDATE hit) = 2 calls.
                    var dbCallsThisAcquire = lk.AcquireRetryCount * 2
                        + (lk.HowAcquired == AcquireMode.TtlSteal ? 2 : 1);
                    Interlocked.Add(ref totalDbCalls, dbCallsThisAcquire);

                    modeLatencies[lk.HowAcquired].Add(sw.ElapsedMilliseconds);
                    if (lk.HowAcquired == AcquireMode.FollowRelease)
                    {
                        followRetryCounts.Add(lk.AcquireRetryCount);
                    }

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref acquires);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    // Timed-out workers also polled, but retryCount is not available
                    // post-throw; conservatively count 0 extra to avoid over-reporting.
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
        var allLatencies  = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec     = acquires * 1000.0 / durationMs;

        _out.WriteLine($"FanIn workers={workerCount,3}: {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"FanIn workers={workerCount,3}: db calls = {totalDbCalls} = {dbCallsPerSec:F0} calls/sec  (delay={delayMs}ms, jitter=on)");
        _out.WriteLine($"FanIn workers={workerCount,3}: latency ms  p50={Pct(allLatencies,50),-7}  p95={Pct(allLatencies,95),-7}  p99={Pct(allLatencies,99),-7}");
        
        EmitDatabaseMetrics(storage);
        EmitModeBreakdown(modeLatencies, followRetryCounts);
    }

    private void EmitDatabaseMetrics(PengdowsCrudJobStorage storage)
    {
        var monitor = storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
        if (monitor != null)
        {
            _out.WriteLine(monitor.GetDatabaseMetricGrid());
        }
    }

    /// <summary>
    /// Returns one latency bag per <see cref="AcquireMode"/> value so that
    /// callers can record acquisition latency separately per code path.
    /// </summary>
    private static Dictionary<AcquireMode, ConcurrentBag<long>> ModeLatencyBags() =>
        Enum.GetValues<AcquireMode>()
            .ToDictionary(m => m, _ => new ConcurrentBag<long>());

    /// <summary>
    /// Writes a per-mode latency breakdown and, for <see cref="AcquireMode.FollowRelease"/>,
    /// retry-count statistics that expose waiter polling cost.
    ///
    /// Example:
    ///   AcquireMode breakdown:
    ///     InsertWin     n=950  p50=0    p95=1    p99=1    max=5
    ///     FollowRelease n=45   p50=3013 p95=9800 p99=13000 max=15069
    ///                   retries: avg=31 p50=31 p95=98 max=143
    ///     TtlSteal      (no samples)
    /// </summary>
    private void EmitModeBreakdown(
        Dictionary<AcquireMode, ConcurrentBag<long>> modeLatencies,
        ConcurrentBag<int>? followRetryCounts = null)
    {
        _out.WriteLine("  AcquireMode breakdown:");
        foreach (var mode in Enum.GetValues<AcquireMode>())
        {
            var sorted = modeLatencies[mode].OrderBy(x => x).ToList();
            if (sorted.Count == 0)
            {
                continue;
            }

            _out.WriteLine(
                $"    {mode,-14} n={sorted.Count,-5}  " +
                $"p50={Pct(sorted, 50),-7}  p95={Pct(sorted, 95),-7}  " +
                $"p99={Pct(sorted, 99),-7}  max={sorted.Last()}");

            if (mode == AcquireMode.FollowRelease && followRetryCounts is { IsEmpty: false })
            {
                var retrySorted = followRetryCounts.OrderBy(x => x).ToList();
                var avg = retrySorted.Average();
                _out.WriteLine(
                    $"    {"retries",-14} avg={avg,-5:F1}  " +
                    $"p50={Pct(retrySorted, 50),-7}  p95={Pct(retrySorted, 95),-7}  " +
                    $"p99={Pct(retrySorted, 99),-7}  max={retrySorted.Last()}");
            }
        }
    }
}
