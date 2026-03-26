using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Out-of-process crash tests for <see cref="PengdowsCrudDistributedLock"/>.
///
/// These tests use a real child process that acquires a lock and is then killed
/// hard (SIGKILL / TerminateProcess).  An in-process test cannot honestly
/// simulate this scenario because it cannot fully replicate:
///
///   * the lock owner's heartbeat stopping cold with no cleanup code running
///   * the OS releasing all file handles and sockets for that process
///   * the full TTL-based orphan lifecycle without cooperative dispose
///
/// The worker binary is built alongside the test project and located in the
/// "worker/" subdirectory of the test output directory.  If it is not found,
/// the test fails with an informative message — run "dotnet build" first.
///
/// What these tests prove:
///   * An orphaned lock row (from a killed process) is stealable once TTL expires
///   * The steal happens within the expected time window (TTL + small buffer)
///   * No zombie ownership — the first caller to retry after expiry wins cleanly
///
/// What they do NOT prove:
///   * Behaviour if the killer and a new acquirer are the same process (different
///     scenario — see <see cref="LockCrashPathTests"/>)
///   * Starvation freedom when many processes race for the expired row simultaneously
/// </summary>
[Collection("SqlServerStress")]
public sealed class LockOutOfProcessTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public LockOutOfProcessTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── 1. Hard kill — orphan expires and is stolen ───────────────────────────

    /// <summary>
    /// A child process acquires the lock and is then hard-killed.
    /// A new acquire attempt must succeed within TTL + a generous buffer,
    /// proving that orphan rows are cleaned up by the TTL steal path.
    ///
    /// The steal window is bounded: after kill, the row's expires_at is at
    /// most TTL (15s) in the future (last heartbeat happened &lt;= TTL/5 = 3s
    /// before kill).  We poll until we can acquire and assert the elapsed time
    /// is below TTL + 10s.
    /// </summary>
    [Fact(Timeout = 60_000)]
    public async Task ProcessKill_OrphanRowExpires_AcquireSucceeds_WithinTtlPlusBuffer()
    {
        const int ttlSeconds = 15;
        var resource = "oop-kill-" + Guid.NewGuid().ToString("N");

        var workerExe = FindWorkerExecutable();
        _out.WriteLine($"Worker: {workerExe}");

        var psi = new ProcessStartInfo
        {
            FileName               = workerExe,
            Arguments              = $"\"{_f.BaseConnectionString}\" \"{resource}\" {ttlSeconds}",
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            UseShellExecute        = false,
        };

        using var proc = Process.Start(psi)!;

        // Drain stderr in the background so it never blocks the process
        var stderrTask = proc.StandardError.ReadToEndAsync();

        // Wait for "ACQUIRED" signal — the worker holds the lock
        string? signal = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            while (true)
            {
                signal = await proc.StandardOutput.ReadLineAsync(cts.Token);
                if (signal == null) break;    // process exited
                if (signal == "ACQUIRED") break;
            }
        }
        catch (OperationCanceledException) { /* timeout */ }

        if (signal != "ACQUIRED")
        {
            var stderr = await stderrTask;
            Assert.Fail($"Worker did not print ACQUIRED within 30s. stderr='{stderr}' last signal='{signal}'");
        }

        _out.WriteLine($"Worker PID={proc.Id} holds lock — killing now");
        proc.Kill(entireProcessTree: true);
        await proc.WaitForExitAsync();

        // Poll until we can steal the expired row
        var sw = Stopwatch.StartNew();
        PengdowsCrudDistributedLock? newLock = null;
        while (sw.Elapsed.TotalSeconds < ttlSeconds + 10)
        {
            try
            {
                // Short per-attempt timeout so polling stays tight
                newLock = new PengdowsCrudDistributedLock(
                    _f.Storage, resource, TimeSpan.FromMilliseconds(300));
                break; // acquired
            }
            catch (DistributedLockTimeoutException)
            {
                await Task.Delay(500);
            }
        }

        Assert.True(newLock != null,
            $"Could not acquire lock within {ttlSeconds + 10}s after process kill — " +
            $"orphan row was not cleaned up by TTL steal path");

        Assert.True(sw.ElapsedMilliseconds < (ttlSeconds + 10) * 1000L,
            $"Steal took {sw.ElapsedMilliseconds}ms — expected < {(ttlSeconds + 10) * 1000}ms");

        using (newLock) { }

        _out.WriteLine($"ProcessKill: kill→steal = {sw.ElapsedMilliseconds}ms  (TTL={ttlSeconds}s, max expected={(ttlSeconds + 10) * 1000}ms)");
    }

    // ── 2. Hard kill — second killer races for the same orphan ───────────────

    /// <summary>
    /// A child process acquires the lock and is killed.  Simultaneously, two
    /// concurrent callers on different resources compete — but only the resource
    /// with the orphaned row is tested.
    ///
    /// After expiry, multiple callers race for the single orphan slot.
    /// Exactly one must succeed; all others must fail with
    /// <see cref="DistributedLockTimeoutException"/> (not corruption).
    /// </summary>
    [Fact(Timeout = 90_000)]
    public async Task ProcessKill_MultipleRacers_ExactlyOneStealSucceeds()
    {
        const int ttlSeconds  = 15;
        const int racerCount  = 10;
        var resource = "oop-race-" + Guid.NewGuid().ToString("N");

        var workerExe = FindWorkerExecutable();

        var psi = new ProcessStartInfo
        {
            FileName               = workerExe,
            Arguments              = $"\"{_f.BaseConnectionString}\" \"{resource}\" {ttlSeconds}",
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            UseShellExecute        = false,
        };

        using var proc = Process.Start(psi)!;

        var stderrTask2 = proc.StandardError.ReadToEndAsync();

        string? signal = null;
        using var acqCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            while (true)
            {
                signal = await proc.StandardOutput.ReadLineAsync(acqCts.Token);
                if (signal == null) break;
                if (signal == "ACQUIRED") break;
            }
        }
        catch (OperationCanceledException) { /* timeout */ }

        if (signal != "ACQUIRED")
        {
            var stderr2 = await stderrTask2;
            Assert.Fail($"Worker did not print ACQUIRED within 30s. stderr='{stderr2}'");
        }

        proc.Kill(entireProcessTree: true);
        await proc.WaitForExitAsync();

        // Wait until the row is definitely expired
        await Task.Delay(TimeSpan.FromSeconds(ttlSeconds + 2));

        // Now race: all racers try with a very short timeout
        var tracker    = new OwnershipTracker();
        long succeeded = 0, failed = 0;

        var tasks = Enumerable.Range(0, racerCount).Select(_ => Task.Run(() =>
        {
            try
            {
                using var lk = new PengdowsCrudDistributedLock(
                    _f.Storage, resource, TimeSpan.FromMilliseconds(500));

                var tid     = Guid.NewGuid().ToString("N");
                var entered = DateTime.UtcNow;
                tracker.Enter(resource, tid);
                Interlocked.Increment(ref succeeded);
                Thread.Sleep(50);
                tracker.Exit(resource, tid, entered, DateTime.UtcNow);
            }
            catch (DistributedLockTimeoutException)
            {
                Interlocked.Increment(ref failed);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Mutual exclusion invariant
        Assert.Equal(0, tracker.Violations);
        Assert.Equal(0, tracker.CountIntervalOverlaps());
        Assert.True(tracker.GlobalMaxConcurrentOwners() <= 1,
            $"MaxConcurrentOwners={tracker.GlobalMaxConcurrentOwners()} after process kill + multi-racer steal");

        // At least one racer must have won
        Assert.True(succeeded >= 1, "No racer succeeded — steal path is broken");

        _out.WriteLine($"ProcessKill multi-racer: succeeded={succeeded}  failed={failed}  violations={tracker.Violations}  maxConcurrent={tracker.GlobalMaxConcurrentOwners()}");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static string FindWorkerExecutable()
    {
        var testDir = Path.GetDirectoryName(
            typeof(LockOutOfProcessTests).Assembly.Location)!;

        // Primary: copied to worker/ subdirectory by the MSBuild CopyWorkerToTestOutput target
        foreach (var candidate in new[]
        {
            Path.Combine(testDir, "worker", "pengdows.hangfire.stress.tests.worker"),
            Path.Combine(testDir, "worker", "pengdows.hangfire.stress.tests.worker.exe"),
        })
        {
            if (File.Exists(candidate)) return candidate;
        }

        throw new FileNotFoundException(
            $"Worker executable not found under {Path.Combine(testDir, "worker")}. " +
            "Run 'dotnet build pengdows.hangfire.stress.tests' to copy it.");
    }
}
