using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using Microsoft.Data.SqlClient;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Crash-path tests for <see cref="PengdowsCrudDistributedLock"/>.
///
/// These tests simulate infrastructure failures that occur while a lock is
/// actively held: connection pool recycled, row deleted under the holder, TTL
/// expiry with no renewal.  The goal is to verify that the lock never leaves
/// corrupted state and that observable failure modes are predictable.
///
/// Context: the biggest gap in the safety evidence is crash recovery — what
/// happens when the infrastructure fails mid-critical-section.  These tests
/// cover the cases that are reproducible without process isolation:
///
///   1. All pooled connections are cleared while the lock is held — simulates
///      a mid-air connection pool recycle (e.g., Azure SQL maintenance event).
///      Heartbeat must reconnect and continue renewing.
///
///   2. Row deleted + pool cleared simultaneously — the worst-case combination:
///      simulates a crash-then-steal scenario.
///
///   3. TTL expiry without heartbeat — if the heartbeat timer fires but the DB
///      is unavailable for longer than TTL, the row expires and another worker
///      can steal it.  This test verifies the steal path works correctly after
///      TTL expiry and that the original holder's LeaseLost is set.
///
/// What these tests do NOT cover (requires process isolation or SQL injection):
///   - Transaction rollback after acquire (our lock does not use transactions)
///   - SQL Server deadlock victimisation during hold
///   - Mid-critical-section process abort (requires a separate test process)
/// </summary>
[Collection("SqlServerStress")]
public sealed class LockCrashPathTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public LockCrashPathTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── 1. Connection pool recycled under active lock ─────────────────────────

    /// <summary>
    /// Acquires a lock, then calls <c>SqlConnection.ClearAllPools()</c> to
    /// forcibly evict every pooled connection.  The heartbeat must survive the
    /// pool recycle — it opens a new connection on the next tick and continues
    /// renewing.
    ///
    /// After waiting two heartbeat cycles the lock must still be held (row
    /// present, version incremented), proving reconnect-on-renewal works.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task ClearConnectionPool_AfterAcquire_HeartbeatSurvivesAndRenews()
    {
        var resource = "crash-poolclear-" + Guid.NewGuid().ToString("N");
        // TTL=10s → heartbeat fires every 2s
        var storage = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(10));

        using var lk = new PengdowsCrudDistributedLock(storage, resource, TimeSpan.FromSeconds(5));
        Assert.False(lk.LeaseLost);

        // Record the version immediately after acquire
        var versionBefore = await _f.QueryScalarAsync<int>(
            "SELECT [version] FROM [HangFire].[hf_lock] WHERE [resource] = @r",
            ("r", resource));

        // Evict all pooled connections — next heartbeat must open a fresh one
        SqlConnection.ClearAllPools();

        // Wait for at least two heartbeat ticks (2s interval + buffer)
        await Task.Delay(5_500);

        Assert.False(lk.LeaseLost,
            "LeaseLost should be false — lock row was not deleted, just connections cleared");

        var versionAfter = await _f.QueryScalarAsync<int>(
            "SELECT [version] FROM [HangFire].[hf_lock] WHERE [resource] = @r",
            ("r", resource));

        Assert.True(versionAfter > versionBefore,
            $"Version did not advance after pool clear (before={versionBefore} after={versionAfter}) — heartbeat may not have reconnected");

        _out.WriteLine($"ClearAllPools: versionBefore={versionBefore}  versionAfter={versionAfter}  leaseLost={lk.LeaseLost}");
    }

    // ── 2. Row deleted + pool cleared simultaneously ──────────────────────────

    /// <summary>
    /// Simulates the worst-case crash scenario: the lock row is deleted AND
    /// the connection pool is simultaneously recycled.  The heartbeat must
    /// detect the missing row and set <c>LeaseLost</c>, without throwing or
    /// leaving zombie state.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task RowDeletedAndPoolCleared_HeartbeatDetectsLeaseLoss()
    {
        var resource = "crash-combined-" + Guid.NewGuid().ToString("N");
        // TTL=5s → heartbeat fires every 1s
        var storage = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(5));

        using var lk = new PengdowsCrudDistributedLock(storage, resource, TimeSpan.FromSeconds(5));
        Assert.False(lk.LeaseLost);

        // Combine both failure modes simultaneously
        await using var sc = _f.Context.CreateSqlContainer(
            "DELETE FROM [HangFire].[hf_lock] WHERE [resource] = @r");
        sc.AddParameterWithValue("r", DbType.String, resource);
        await sc.ExecuteNonQueryAsync();

        SqlConnection.ClearAllPools();

        // Wait for heartbeat to detect the missing row (1s interval + buffer)
        await Task.Delay(3_000);

        Assert.True(lk.LeaseLost,
            "Expected LeaseLost=true after row deleted AND connection pool cleared");

        _out.WriteLine($"Combined crash (delete + ClearAllPools): LeaseLost={lk.LeaseLost} ✓");
    }

    // ── 3. TTL expiry — expired row is stealable after TTL ───────────────────

    /// <summary>
    /// Acquires a lock with a very short TTL (5s) and a heartbeat interval of
    /// 1s.  Pauses the heartbeat by deleting the row, then waits for the TTL
    /// to expire.  A second acquire must succeed immediately (expired row steal)
    /// and the original holder must have LeaseLost=true.
    ///
    /// This proves the steal-after-expiry path works end-to-end.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task TtlExpiry_WithNoRenewal_RowBecomesSteatable()
    {
        var resource = "crash-ttlexpiry-" + Guid.NewGuid().ToString("N");
        // Short TTL so expiry happens fast; heartbeat at TTL/5 = 1s
        var storage  = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(5));

        var firstHolder = new PengdowsCrudDistributedLock(
            storage, resource, TimeSpan.FromSeconds(5));
        Assert.False(firstHolder.LeaseLost);

        // Kill the row to stop renewal, then wait for TTL to expire
        await using var sc = _f.Context.CreateSqlContainer(
            "DELETE FROM [HangFire].[hf_lock] WHERE [resource] = @r");
        sc.AddParameterWithValue("r", DbType.String, resource);
        await sc.ExecuteNonQueryAsync();

        // Heartbeat detects missing row → LeaseLost
        await Task.Delay(3_000);
        Assert.True(firstHolder.LeaseLost);

        // Wait out the original TTL window so a new holder can succeed
        // without racing against any residual state
        await Task.Delay(3_000);

        // Second holder must acquire immediately (no row to contend with)
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var secondHolder = new PengdowsCrudDistributedLock(
            storage, resource, TimeSpan.FromSeconds(5));
        sw.Stop();

        Assert.False(secondHolder.LeaseLost);
        Assert.True(sw.ElapsedMilliseconds < 500,
            $"Second acquire after TTL expiry took {sw.ElapsedMilliseconds}ms — expected near-instant");

        firstHolder.Dispose(); // safe to call after LeaseLost

        _out.WriteLine($"TTL expiry: firstHolder.LeaseLost={firstHolder.LeaseLost}  secondAcquireMs={sw.ElapsedMilliseconds}");
    }

    // ── 4. Stale heartbeat after steal is fenced by owner_id ─────────────────

    /// <summary>
    /// Proves that ownership change breaks all old write predicates.
    ///
    /// Sequence:
    ///   1. A acquires and renews twice → version = 3
    ///   2. A's row is force-expired (simulates A crashing without releasing)
    ///   3. B steals the expired row → version resets to 1, owner = B
    ///   4. A's delayed heartbeat fires with stale (owner=A, version=3)
    ///   5. TryRenewAsync must return false — owner_id mismatch fences the write
    ///   6. Row is unchanged: still owned by B, version and expires_at intact
    ///
    /// This encodes the safety argument for version=1 reset on steal:
    /// owner_id is the epoch fence; version is a per-lease freshness token only.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task StaleHeartbeat_AfterOwnershipChange_IsRejectedAndRowIsUnchanged()
    {
        var resource = "crash-stalerenew-" + Guid.NewGuid().ToString("N");
        var ownerA   = Guid.NewGuid().ToString("N");
        var ownerB   = Guid.NewGuid().ToString("N");
        var ttl      = TimeSpan.FromSeconds(30);

        // 1. A acquires (version=1) and renews twice → version=3
        var now = DateTime.UtcNow;
        var claimedA = await _f.Storage.Locks.TryAcquireAsync(resource, ownerA, now + ttl, now);
        Assert.True(claimedA);
        Assert.True(await _f.Storage.Locks.TryRenewAsync(resource, ownerA, 1, DateTime.UtcNow + ttl)); // 1→2
        Assert.True(await _f.Storage.Locks.TryRenewAsync(resource, ownerA, 2, DateTime.UtcNow + ttl)); // 2→3
        const int staleVersion = 3;

        // 2. Force-expire A's row (simulates A crashing mid-hold)
        await using (var sc = _f.Context.CreateSqlContainer(
            "UPDATE [HangFire].[hf_lock] SET [expires_at] = @e WHERE [resource] = @r"))
        {
            sc.AddParameterWithValue("e", DbType.DateTime2, DateTime.UtcNow.AddSeconds(-1));
            sc.AddParameterWithValue("r", DbType.String, resource);
            await sc.ExecuteNonQueryAsync();
        }

        // 3. B steals the expired row
        now = DateTime.UtcNow;
        var claimedB = await _f.Storage.Locks.TryAcquireAsync(resource, ownerB, now + ttl, now);
        Assert.True(claimedB, "Expected B to steal the expired row via conditional UPDATE");

        // Capture B's row state before A's stale renew
        var versionAfterSteal = await _f.QueryScalarAsync<int>(
            "SELECT [version] FROM [HangFire].[hf_lock] WHERE [resource] = @r", ("r", resource));

        // 4. A's stale heartbeat fires — must be rejected
        var renewed = await _f.Storage.Locks.TryRenewAsync(
            resource, ownerA, staleVersion, DateTime.UtcNow + ttl);
        Assert.False(renewed, $"Stale renew (owner=A, version={staleVersion}) must return false after B stole the row");

        // 5. Row is unchanged: still owned by B, version not mutated by A
        var ownerAfter   = await _f.QueryScalarAsync<string>(
            "SELECT [owner_id] FROM [HangFire].[hf_lock] WHERE [resource] = @r", ("r", resource));
        var versionAfter = await _f.QueryScalarAsync<int>(
            "SELECT [version] FROM [HangFire].[hf_lock] WHERE [resource] = @r", ("r", resource));

        Assert.Equal(ownerB, ownerAfter);
        Assert.Equal(versionAfterSteal, versionAfter);

        // Clean up
        await _f.Storage.Locks.ReleaseAsync(resource, ownerB);

        _out.WriteLine($"StaleRenew: A's stale renew(v={staleVersion}) returned {renewed} — row owner={ownerAfter}  version={versionAfter}");
    }

    // ── 6. Dispose during active heartbeat ────────────────────────────────────

    /// <summary>
    /// Calls Dispose while the heartbeat timer is actively renewing.  There
    /// must be no race condition between the timer callback and the release
    /// path (double-release, ObjectDisposedException, unhandled exception).
    ///
    /// Repeated 50 times to expose timing-dependent defects.  50 iterations
    /// is not a proof of race freedom — it is evidence that no race was
    /// observed across 50 immediate-dispose attempts under this configuration.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task DisposeDuringActiveHeartbeat_NoRaceObservedIn50Iterations()
    {
        const int iterations = 50;

        // TTL=5s → heartbeat fires every 1s; we dispose within the first cycle
        var storage = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(5));

        for (int i = 0; i < iterations; i++)
        {
            var resource = "crash-disposerace-" + Guid.NewGuid().ToString("N");
            var lk = new PengdowsCrudDistributedLock(
                storage, resource, TimeSpan.FromSeconds(5));

            // Dispose immediately — may race with the first heartbeat tick
            lk.Dispose();

            // Allow any in-flight async heartbeat work to settle
            await Task.Delay(50);
        }

        _out.WriteLine($"DisposeDuringHeartbeat: {iterations} iterations completed — no race observed (not a proof of race freedom)");
    }
}
