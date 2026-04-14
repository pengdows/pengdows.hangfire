using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using pengdows.crud.exceptions;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Boundary-pressure tests: connection pool size is set well below worker
/// count.  The goal is NOT to prove lock performance under load — it is to
/// prove that pool saturation never corrupts lock state.
///
/// Expected behaviour:
///   Workers that cannot get a DB connection receive
///   <see cref="PoolSaturatedException"/> (: TimeoutException).
///   Workers that get a connection but find the lock taken and time out
///   receive <see cref="DistributedLockTimeoutException"/>.
///   Workers that succeed must never overlap.
///
/// If MaxConcurrentOwners > 1, or Violations > 0, or IntervalOverlaps > 0,
/// the lock is broken.  Full stop.
///
/// These tests are in the same xUnit collection as <see cref="LockStressTests"/>
/// so they share the container fixture and avoid spinning up a second SQL Server.
/// </summary>
[Collection("SqlServerStress")]
public sealed class BoundaryPressureTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public BoundaryPressureTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── 1. Single hot key — pool well below worker count ─────────────────────

    /// <summary>
    /// 200 workers all target the same resource using a context whose pool
    /// size is 40.  Roughly 160 workers will be turned away by the pool
    /// governor or will time out waiting for the lock.
    ///
    /// All 200 workers begin at the same instant (<see cref="Barrier"/>).
    /// Dedicated threads (not thread-pool) prevent scheduler-induced
    /// serialization from masking true contention.
    ///
    /// The invariant: among the workers that DO successfully acquire the lock,
    /// zero overlaps — no corruption, no phantom holders.
    /// </summary>
    [Fact(Timeout = 120_000)]
    public async Task ConstrainedPool_SingleResource_NoMutexViolation()
    {
        const int workerCount = 200;
        const int poolSize    = 40;

        var resource = "bp-single-" + Guid.NewGuid().ToString("N");
        var storage  = _f.CreateStorageWithPoolSize(poolSize, TimeSpan.FromSeconds(30));
        var tracker  = new OwnershipTracker();

        // Pre-create the lock row so this test exercises pool-pressure contention
        // against an existing resource, not the empty-row cold-start path.
        using (var warmup = new PengdowsCrudDistributedLock(
                   storage, resource, TimeSpan.FromSeconds(5)))
        {
        }

        long started = 0, completed = 0;
        long succeeded = 0, lockTimedOut = 0, poolRejected = 0;
        var unexpectedExceptions = new ConcurrentBag<Exception>();

        // Barrier ensures all workers begin contending at the same clock tick.
        var barrier = new Barrier(workerCount);

        var threads = Enumerable.Range(0, workerCount).Select(_ =>
        {
            var t = new Thread(() =>
            {
                // All workers arrive here; last one releases all simultaneously.
                barrier.SignalAndWait();
                Interlocked.Increment(ref started);
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        storage, resource, TimeSpan.FromSeconds(5));

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(resource, tid);

                    Interlocked.Increment(ref succeeded);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(resource, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref lockTimedOut);
                }
                catch (Exception ex)
                {
                    if (IsExpectedInfraFailure(ex))
                    {
                        Interlocked.Increment(ref poolRejected);
                    }
                    else
                    {
                        unexpectedExceptions.Add(ex);
                        Interlocked.Increment(ref poolRejected); // still counted
                    }
                }
                finally
                {
                    Interlocked.Increment(ref completed);
                }
            }) { IsBackground = true };
            t.Start();
            return t;
        }).ToArray();

        await Task.Run(() => { foreach (var t in threads) t.Join(); });

        // ── accounting invariants ───────────────────────────────────────────
        // Every worker must have started and completed — no lost threads.
        Assert.Equal(workerCount, (int)started);
        Assert.Equal(workerCount, (int)completed);
        Assert.Equal(workerCount, (int)(succeeded + lockTimedOut + poolRejected));

        // No unexpected exception types — if this fires, a real bug is hiding.
        Assert.True(unexpectedExceptions.IsEmpty,
            "Unexpected exceptions during pool-pressure test:\n" +
            string.Join("\n", unexpectedExceptions.Select(SummarizeException)));

        // ── lock correctness invariants ─────────────────────────────────────
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated under pool pressure");

        // Lock must still have been functional for at least one worker.
        Assert.True(succeeded > 0,
            "No workers acquired the lock — lock non-functional under pool pressure");

        _out.WriteLine($"ConstrainedPool single-resource (pool={poolSize}, workers={workerCount}):");
        _out.WriteLine($"  succeeded={succeeded}  lockTimedOut={lockTimedOut}  poolRejected={poolRejected}");
        _out.WriteLine($"  Violations={tracker.Violations}  Overlaps={tracker.CountIntervalOverlaps()}  MaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        
        EmitDatabaseMetrics(storage);
        EmitExceptionSummary(unexpectedExceptions);
    }

    // ── 2. Hot-key skew — pool below worker count ─────────────────────────────

    /// <summary>
    /// 200 workers across 5 resources (80 % hitting the first 2 "hot" keys),
    /// pool size = 40.  Per-resource invariant must hold for every key even
    /// when pool pressure is applied asymmetrically due to skew.
    /// </summary>
    [Fact(Timeout = 180_000)]
    public async Task ConstrainedPool_HotKeySkew_NoMutexViolationPerResource()
    {
        const int workerCount   = 200;
        const int resourceCount = 5;
        const int poolSize      = 40;

        var prefix    = "bp-skew-" + Guid.NewGuid().ToString("N") + "-";
        var resources = Enumerable.Range(0, resourceCount).Select(i => prefix + i).ToArray();
        var storage   = _f.CreateStorageWithPoolSize(poolSize, TimeSpan.FromSeconds(30));
        var tracker   = new OwnershipTracker();

        // Seed every resource once so the test isolates skewed contention under
        // pool pressure instead of mixing in first-writer row creation races.
        foreach (var resource in resources)
        {
            using var warmup = new PengdowsCrudDistributedLock(
                storage, resource, TimeSpan.FromSeconds(5));
        }

        long started = 0, completed = 0;
        long succeeded = 0, lockTimedOut = 0, poolRejected = 0;
        var unexpectedExceptions = new ConcurrentBag<Exception>();

        // Pre-compute resource assignments before thread creation so the
        // closure captures the value, not the loop variable.
        var assignments = Enumerable.Range(0, workerCount).Select(i =>
            i < (int)(workerCount * 0.8)
                ? resources[i % 2]
                : resources[2 + (i % (resourceCount - 2))]).ToArray();

        var barrier = new Barrier(workerCount);

        var threads = Enumerable.Range(0, workerCount).Select(i =>
        {
            var res = assignments[i];
            var t = new Thread(() =>
            {
                barrier.SignalAndWait();
                Interlocked.Increment(ref started);
                try
                {
                    using var lk = new PengdowsCrudDistributedLock(
                        storage, res, TimeSpan.FromSeconds(5));

                    var tid     = Guid.NewGuid().ToString("N");
                    var entered = DateTime.UtcNow;
                    tracker.Enter(res, tid);

                    Interlocked.Increment(ref succeeded);
                    Thread.Sleep(Random.Shared.Next(10, 40));

                    tracker.Exit(res, tid, entered, DateTime.UtcNow);
                }
                catch (DistributedLockTimeoutException)
                {
                    Interlocked.Increment(ref lockTimedOut);
                }
                catch (Exception ex)
                {
                    if (IsExpectedInfraFailure(ex))
                    {
                        Interlocked.Increment(ref poolRejected);
                    }
                    else
                    {
                        unexpectedExceptions.Add(ex);
                        Interlocked.Increment(ref poolRejected);
                    }
                }
                finally
                {
                    Interlocked.Increment(ref completed);
                }
            }) { IsBackground = true };
            t.Start();
            return t;
        }).ToArray();

        await Task.Run(() => { foreach (var t in threads) t.Join(); });

        // ── accounting invariants ───────────────────────────────────────────
        Assert.Equal(workerCount, (int)started);
        Assert.Equal(workerCount, (int)completed);
        Assert.Equal(workerCount, (int)(succeeded + lockTimedOut + poolRejected));

        Assert.True(unexpectedExceptions.IsEmpty,
            "Unexpected exceptions during hot-skew pool-pressure test:\n" +
            string.Join("\n", unexpectedExceptions.Select(SummarizeException)));

        // ── lock correctness invariants ─────────────────────────────────────
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} — mutual exclusion violated under skewed pool pressure");

        Assert.Equal(workerCount, (int)(succeeded + lockTimedOut + poolRejected));

        var perResourceMax = resources.Select(r =>
            $"{r.Split('-').Last()}={tracker.MaxConcurrentOwners(r)}").ToList();

        _out.WriteLine($"ConstrainedPool hot-skew (pool={poolSize}, workers={workerCount}, resources={resourceCount}):");
        _out.WriteLine($"  succeeded={succeeded}  lockTimedOut={lockTimedOut}  poolRejected={poolRejected}");
        _out.WriteLine($"  Violations={tracker.Violations}  Overlaps={tracker.CountIntervalOverlaps()}  GlobalMaxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
        _out.WriteLine($"  Per-resource maxConcurrent: {string.Join("  ", perResourceMax)}");
        EmitExceptionSummary(unexpectedExceptions);
    }

    // ── 3. Post-run orphan check ──────────────────────────────────────────────

    /// <summary>
    /// Verifies that a previously-constrained context releases cleanly:
    /// after all workers finish, no orphan rows remain in hf_lock for any
    /// resource used in this test run.
    ///
    /// This proves that pool backpressure does not leave zombie lock rows.
    /// </summary>
    [Fact(Timeout = 60_000)]
    public async Task ConstrainedPool_AfterRun_NoOrphanLockRows()
    {
        const int workerCount = 50;
        const int poolSize    = 20;

        var resource = "bp-orphan-" + Guid.NewGuid().ToString("N");
        var storage  = _f.CreateStorageWithPoolSize(poolSize, TimeSpan.FromSeconds(15));
        long succeeded = 0;

        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            try
            {
                using var lk = new PengdowsCrudDistributedLock(
                    storage, resource, TimeSpan.FromSeconds(3));
                Interlocked.Increment(ref succeeded);
                Thread.Sleep(20);
            }
            catch (DistributedLockTimeoutException) { }
            catch (Exception) { }
        })).ToArray();

        await Task.WhenAll(tasks);

        // No lock rows should survive — every Dispose must have released.
        var count = await _f.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM [HangFire].[hf_lock] WHERE [resource] = @r",
            ("r", resource));

        Assert.Equal(0, count);

        _out.WriteLine($"ConstrainedPool orphan check: succeeded={succeeded}  lockRowsRemaining={count}");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns true for exception types that are expected under intentional
    /// pool-pressure conditions and do not indicate a lock correctness defect:
    /// <list type="bullet">
    ///   <item><description><see cref="DistributedLockTimeoutException"/> — lock was held by another worker</description></item>
    ///   <item><description><see cref="TimeoutException"/> — pool governor (<see cref="PoolSaturatedException"/>) or semaphore timeout</description></item>
    ///   <item><description><see cref="DbException"/> — SQL Server connection rejection under load</description></item>
    ///   <item><description><see cref="InvalidOperationException"/> mentioning "pool" — ADO.NET pool exhausted</description></item>
    /// </list>
    /// Anything else is a candidate bug: <see cref="NullReferenceException"/>,
    /// <see cref="ArgumentException"/>, corruption exceptions, etc.
    /// </summary>
    private static bool IsExpectedInfraFailure(Exception ex) =>
        ex is DistributedLockTimeoutException
        || ex is TimeoutException          // PoolSaturatedException : TimeoutException
        || ex is CommandTimeoutException   // pengdows.crud writer-pool saturation
        || ex is DbException               // SqlException, SqliteException, etc.
        || (ex is InvalidOperationException ioe
            && ioe.Message.IndexOf("pool", StringComparison.OrdinalIgnoreCase) >= 0);

    private static string SummarizeException(Exception ex) =>
        $"  {ex.GetType().Name}: {ex.Message.Split('\n')[0]}";

    private void EmitExceptionSummary(ConcurrentBag<Exception> bag)
    {
        if (bag.IsEmpty)
        {
            return;
        }

        var byType = bag
            .GroupBy(e => e.GetType().Name)
            .Select(g => $"  {g.Key} ×{g.Count()}: {g.First().Message.Split('\n')[0]}");
        _out.WriteLine("  UNEXPECTED exceptions:");
        foreach (var line in byType)
        {
            _out.WriteLine(line);
        }
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
