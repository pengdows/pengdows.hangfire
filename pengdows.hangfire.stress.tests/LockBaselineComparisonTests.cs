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
/// Side-by-side comparison of our <see cref="PengdowsCrudDistributedLock"/>
/// against Hangfire's stock SQL Server lock (<c>sp_getapplock</c>) running
/// against the same container with the same worker counts, pool sizes, and
/// hold durations.
///
/// ## What this proves (when both pass)
///
/// Both implementations maintain mutual exclusion under the same load.
/// The comparison does NOT prove one is "better" in a general sense — it
/// proves they are both correct under the tested scenarios and gives
/// side-by-side latency and throughput numbers so differences can be observed.
///
/// ## What it does NOT prove
///
/// * Superiority in scenarios not tested here
/// * Behaviour under different SQL Server versions or configurations
/// * Real-world performance outside this container (network, I/O, CPU differ)
///
/// ## Interpreting the output
///
/// Run "dotnet test --logger 'console;verbosity=detailed'" and compare the
/// [ours] and [stock] lines for the same scenario.  Differences in p95/p99
/// latency are the most meaningful signals.
/// </summary>
[Collection("SqlServerStress")]
public sealed class LockBaselineComparisonTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public LockBaselineComparisonTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── 1. N=50 burst — mutual exclusion + latency ───────────────────────────

    [Fact(Timeout = 120_000)]
    public async Task Burst_50Workers_OurImpl_ZeroViolations()
    {
        await RunBurstScenario(
            label:       "ours",
            workerCount: 50,
            acquireLock: (resource, timeout) =>
                new PengdowsCrudDistributedLock(_f.Storage, resource, timeout));
    }

    [Fact(Timeout = 120_000)]
    public async Task Burst_50Workers_HangfireSqlServer_ZeroViolations()
    {
        await RunBurstScenario(
            label:       "stock",
            workerCount: 50,
            acquireLock: (resource, timeout) =>
            {
                var conn = _f.BaselineStorage.GetConnection();
                var lk   = conn.AcquireDistributedLock(resource, timeout);
                return new CombinedDisposable(lk, conn); // release lock then connection
            });
    }

    // ── 2. Short sustained — 30s, 10 workers ─────────────────────────────────

    [Fact(Timeout = 90_000)]
    public async Task ShortSustained_10Workers_30Seconds_OurImpl()
    {
        await RunSustainedScenario(
            label:      "ours",
            workers:    10,
            durationMs: 30_000,
            acquireLock: (resource, timeout) =>
                new PengdowsCrudDistributedLock(_f.Storage, resource, timeout));
    }

    [Fact(Timeout = 90_000)]
    public async Task ShortSustained_10Workers_30Seconds_HangfireSqlServer()
    {
        await RunSustainedScenario(
            label:      "stock",
            workers:    10,
            durationMs: 30_000,
            acquireLock: (resource, timeout) =>
            {
                var conn = _f.BaselineStorage.GetConnection();
                var lk   = conn.AcquireDistributedLock(resource, timeout);
                return new CombinedDisposable(lk, conn);
            });
    }

    // ── shared scenarios ─────────────────────────────────────────────────────

    private async Task RunBurstScenario(
        string label,
        int    workerCount,
        Func<string, TimeSpan, IDisposable> acquireLock)
    {
        var resource      = "cmp-burst-" + Guid.NewGuid().ToString("N");
        var tracker       = new OwnershipTracker();
        var modeLatencies = ModeLatencyBags();
        var retryCounts   = new ConcurrentBag<int>();
        long totalDbCalls = 0;
        long acquired = 0, timeouts = 0;
        var startGate = new ManualResetEventSlim(false);

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            startGate.Wait();
            var sw = Stopwatch.StartNew();
            try
            {
                using var lk = acquireLock(resource, TimeSpan.FromSeconds(90));
                var elapsed = sw.ElapsedMilliseconds;

                // Extract internal metrics when the lock is ours.
                if (lk is PengdowsCrudDistributedLock ours)
                {
                    modeLatencies[ours.HowAcquired].Add(elapsed);
                    retryCounts.Add(ours.AcquireRetryCount);
                    var dbCalls = ours.AcquireRetryCount * 2L
                        + (ours.HowAcquired == AcquireMode.TtlSteal ? 2 : 1);
                    Interlocked.Add(ref totalDbCalls, dbCalls);
                }
                else
                {
                    modeLatencies[AcquireMode.InsertWin].Add(elapsed); // bucket stock into fast path
                }

                var tid     = Guid.NewGuid().ToString("N");
                var entered = DateTime.UtcNow;
                tracker.Enter(resource, tid);
                Interlocked.Increment(ref acquired);
                Thread.Sleep(Random.Shared.Next(10, 50));
                tracker.Exit(resource, tid, entered, DateTime.UtcNow);
            }
            catch (DistributedLockTimeoutException)
            {
                Interlocked.Increment(ref timeouts);
            }
        })).ToArray();

        startGate.Set();
        await Task.WhenAll(tasks);

        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"[{label}] MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");
        Assert.Equal(0, timeouts);

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        _out.WriteLine($"[{label}] burst workers={workerCount}  acquired={acquired}  timeouts={timeouts}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _out.WriteLine($"[{label}] acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        EmitOurMetrics(label, totalDbCalls, durationMs: null, modeLatencies, retryCounts);
    }

    private async Task RunSustainedScenario(
        string label,
        int    workers,
        int    durationMs,
        Func<string, TimeSpan, IDisposable> acquireLock)
    {
        var resource      = "cmp-sust-" + Guid.NewGuid().ToString("N");
        var tracker       = new OwnershipTracker();
        var modeLatencies = ModeLatencyBags();
        var retryCounts   = new ConcurrentBag<int>();
        long totalDbCalls = 0;
        long acquires = 0, timeouts = 0;
        var cts = new CancellationTokenSource(durationMs);

        var tasks = Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var sw = Stopwatch.StartNew();
                try
                {
                    using var lk = acquireLock(resource, TimeSpan.FromSeconds(15));
                    var elapsed = sw.ElapsedMilliseconds;

                    if (lk is PengdowsCrudDistributedLock ours)
                    {
                        modeLatencies[ours.HowAcquired].Add(elapsed);
                        retryCounts.Add(ours.AcquireRetryCount);
                        var dbCalls = ours.AcquireRetryCount * 2L
                        + (ours.HowAcquired == AcquireMode.TtlSteal ? 2 : 1);
                    Interlocked.Add(ref totalDbCalls, dbCalls);
                    }
                    else
                    {
                        modeLatencies[AcquireMode.InsertWin].Add(elapsed);
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
            $"[{label}] MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated");

        var allLatencies = modeLatencies.Values.SelectMany(b => b).OrderBy(x => x).ToList();
        var opsPerSec    = acquires * 1000.0 / durationMs;
        _out.WriteLine($"[{label}] sustained({durationMs / 1000}s): {acquires} acquires = {opsPerSec:F1} ops/sec  timeouts={timeouts}");
        _out.WriteLine($"[{label}] acquire-latency ms  p50={Pct(allLatencies,50)}  p95={Pct(allLatencies,95)}  p99={Pct(allLatencies,99)}  max={allLatencies.LastOrDefault()}");
        _out.WriteLine($"[{label}] violations={tracker.Violations}  overlaps={tracker.CountIntervalOverlaps()}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        EmitOurMetrics(label, totalDbCalls, durationMs, modeLatencies, retryCounts);
    }

    private static Dictionary<AcquireMode, ConcurrentBag<long>> ModeLatencyBags() =>
        Enum.GetValues<AcquireMode>().ToDictionary(m => m, _ => new ConcurrentBag<long>());

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

    /// <summary>
    /// Emits the pengdows.crud-specific internal metrics when the lock under
    /// test is ours.  Silently skips for the stock implementation (no data).
    /// </summary>
    private void EmitOurMetrics(
        string label,
        long   totalDbCalls,
        int?   durationMs,
        Dictionary<AcquireMode, ConcurrentBag<long>> modeLatencies,
        ConcurrentBag<int> retryCounts)
    {
        if (totalDbCalls == 0)
        {
            return; // stock side — nothing to emit
        }

        if (durationMs.HasValue)
        {
            var dbCallsPerSec = totalDbCalls * 1000.0 / durationMs.Value;
            _out.WriteLine($"[{label}] db calls = {totalDbCalls}  ({dbCallsPerSec:F0} calls/sec)");
        }
        else
        {
            _out.WriteLine($"[{label}] db calls = {totalDbCalls}");
        }

        // Print the full metrics grid if we're using pengdows.crud
        var monitor = _f.Storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
        if (monitor != null)
        {
            _out.WriteLine(monitor.GetDatabaseMetricGrid());
        }

        _out.WriteLine($"[{label}] AcquireMode breakdown:");
        foreach (var mode in Enum.GetValues<AcquireMode>())
        {
            var sorted = modeLatencies[mode].OrderBy(x => x).ToList();
            if (sorted.Count == 0) continue;
            _out.WriteLine($"[{label}]   {mode,-14} n={sorted.Count,-5}  p50={Pct(sorted,50),-7}  p95={Pct(sorted,95),-7}  p99={Pct(sorted,99),-7}  max={sorted.Last()}");
        }

        if (!retryCounts.IsEmpty)
        {
            var rs = retryCounts.OrderBy(x => x).ToList();
            _out.WriteLine($"[{label}]   retries        avg={rs.Average(),-5:F1}  p50={Pct(rs,50),-7}  p95={Pct(rs,95),-7}  p99={Pct(rs,99),-7}  max={rs.Last()}");
        }
    }

    /// <summary>
    /// Disposes two <see cref="IDisposable"/>s in order: <paramref name="first"/>
    /// then <paramref name="second"/>.  Used to release a Hangfire lock handle
    /// before closing the connection it was acquired on.
    /// </summary>
    private sealed class CombinedDisposable : IDisposable
    {
        private readonly IDisposable _first;
        private readonly IDisposable _second;

        public CombinedDisposable(IDisposable first, IDisposable second)
        {
            _first  = first;
            _second = second;
        }

        public void Dispose()
        {
            _first.Dispose();
            _second.Dispose();
        }
    }
}
