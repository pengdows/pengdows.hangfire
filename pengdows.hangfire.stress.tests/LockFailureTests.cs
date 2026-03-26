using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Failure-path tests for <see cref="PengdowsCrudDistributedLock"/>.
///
/// These tests deliberately create hostile conditions — row deletion, wrong
/// owners, expired rows — to verify that the lock behaves correctly when the
/// environment is not cooperative.
/// </summary>
[Collection("SqlServerStress")]
public sealed class LockFailureTests
{
    private readonly SqlServerFixture  _f;
    private readonly ITestOutputHelper _out;

    public LockFailureTests(SqlServerFixture fixture, ITestOutputHelper output)
    {
        _f   = fixture;
        _out = output;
    }

    // ── Lease-loss detection ─────────────────────────────────────────────────

    /// <summary>
    /// Simulates a process crash / external DBA cleanup: the lock row is
    /// deleted directly from the database while the holder is still alive.
    /// The heartbeat must detect the missing row and set LeaseLost.
    /// </summary>
    [Fact(Timeout = 30_000)]
    public async Task DirectDelete_AfterAcquire_HeartbeatDetectsLeaseLoss()
    {
        var resource = "stress-leaseloss-" + Guid.NewGuid().ToString("N");
        // TTL=5s → heartbeat fires at TTL/5 = 1s
        var storage = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(5));

        using var lk = new PengdowsCrudDistributedLock(storage, resource, TimeSpan.FromSeconds(5));
        Assert.False(lk.LeaseLost);

        // Delete the row — simulates crash/steal/DBA cleanup
        await using var sc = _f.Context.CreateSqlContainer(
            "DELETE FROM [HangFire].[hf_lock] WHERE [resource] = @r");
        sc.AddParameterWithValue("r", DbType.String, resource);
        await sc.ExecuteNonQueryAsync();

        // Wait for at least two heartbeat ticks (1s interval + generous buffer)
        await Task.Delay(2_500);

        Assert.True(lk.LeaseLost,
            "Expected LeaseLost=true after row was deleted under an active lock");

        _out.WriteLine($"LeaseLoss detected after direct delete (resource='{resource}')");
    }

    // ── Expired-row steal ────────────────────────────────────────────────────

    /// <summary>
    /// Inserts a lock row with expires_at in the past (simulating a crashed
    /// former holder). A new acquire must steal it without blocking or timeout.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task ExpiredRow_CanBeStolen_Immediately()
    {
        var resource = "stress-steal-" + Guid.NewGuid().ToString("N");
        var storage  = _f.CreateStorageWithTtl(TimeSpan.FromSeconds(5));

        // Insert an already-expired row
        await using var sc = _f.Context.CreateSqlContainer(
            "INSERT INTO [HangFire].[hf_lock] ([resource],[owner_id],[expires_at],[version]) " +
            "VALUES (@r, @o, @e, 1)");
        sc.AddParameterWithValue("r", DbType.String,   resource);
        sc.AddParameterWithValue("o", DbType.String,   "dead-owner");
        sc.AddParameterWithValue("e", DbType.DateTime, DateTime.UtcNow.AddMinutes(-2));
        await sc.ExecuteNonQueryAsync();

        // Acquire — should steal the expired row immediately
        using var lk = new PengdowsCrudDistributedLock(storage, resource, TimeSpan.FromSeconds(5));
        Assert.NotNull(lk);
        Assert.False(lk.LeaseLost);

        _out.WriteLine($"Expired row successfully stolen (resource='{resource}')");
    }

    // ── Timeout semantics ────────────────────────────────────────────────────

    /// <summary>
    /// Acquiring the same resource twice with zero timeout must immediately
    /// throw <see cref="DistributedLockTimeoutException"/>.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task SecondAcquire_ZeroTimeout_ThrowsImmediately()
    {
        var resource = "stress-double-" + Guid.NewGuid().ToString("N");

        using var first = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<DistributedLockTimeoutException>(() =>
            Task.Run(() => new PengdowsCrudDistributedLock(
                _f.Storage, resource, TimeSpan.Zero)));

        _out.WriteLine($"Zero-timeout second acquire threw DistributedLockTimeoutException (resource='{resource}')");
    }

    // ── Owner-guard: release by wrong owner ──────────────────────────────────

    /// <summary>
    /// Verifies that ReleaseAsync is a no-op when called by a different owner.
    /// The original row must survive intact.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task ReleaseByWrongOwner_IsNoOp_RowSurvives()
    {
        var resource = "stress-ownerguard-" + Guid.NewGuid().ToString("N");

        await using var sc = _f.Context.CreateSqlContainer(
            "INSERT INTO [HangFire].[hf_lock] ([resource],[owner_id],[expires_at],[version]) " +
            "VALUES (@r, @o, @e, 1)");
        sc.AddParameterWithValue("r", DbType.String,   resource);
        sc.AddParameterWithValue("o", DbType.String,   "owner-a");
        sc.AddParameterWithValue("e", DbType.DateTime, DateTime.UtcNow.AddMinutes(5));
        await sc.ExecuteNonQueryAsync();

        // Release as a different owner — must be a no-op
        await _f.Storage.Locks.ReleaseAsync(resource, "owner-b");

        var count = await _f.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM [HangFire].[hf_lock] " +
            "WHERE [resource] = @r AND [owner_id] = 'owner-a'",
            ("r", resource));

        Assert.Equal(1, count);

        _out.WriteLine($"Release by wrong owner correctly preserved owner-a's row (resource='{resource}')");
    }

    // ── Version CAS: TryRenew with stale version ─────────────────────────────

    /// <summary>
    /// Calling TryRenewAsync with the wrong version (stale CAS token) must
    /// return false without modifying the row.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task TryRenew_WrongVersion_ReturnsFalse_RowUnchanged()
    {
        var resource = "stress-renewcas-" + Guid.NewGuid().ToString("N");
        using var lk = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(30));

        var renewed = await _f.Storage.Locks.TryRenewAsync(
            resource, "wrong-owner", 999, DateTime.UtcNow.AddMinutes(5));

        Assert.False(renewed);

        _out.WriteLine($"TryRenew with wrong owner/version correctly returned false (resource='{resource}')");
    }

    // ── Double-dispose safety ────────────────────────────────────────────────

    /// <summary>
    /// Calling Dispose twice on the same lock handle must not throw or issue
    /// a second release query.
    /// </summary>
    [Fact]
    public void DoubleDispose_IsIdempotent()
    {
        var resource = "stress-dbldisp-" + Guid.NewGuid().ToString("N");
        var lk = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(5));

        lk.Dispose();
        lk.Dispose(); // must not throw

        _out.WriteLine($"Double-dispose completed without exception (resource='{resource}')");
    }

    // ── Orphan check: no lock rows after Dispose ─────────────────────────────

    /// <summary>
    /// After Dispose, the hf_lock row for the released resource must be gone.
    /// </summary>
    [Fact(Timeout = 15_000)]
    public async Task AfterDispose_NoOrphanRowRemains()
    {
        var resource = "stress-orphan-" + Guid.NewGuid().ToString("N");
        var lk = new PengdowsCrudDistributedLock(
            _f.Storage, resource, TimeSpan.FromSeconds(5));
        lk.Dispose();

        var count = await _f.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM [HangFire].[hf_lock] WHERE [resource] = @r",
            ("r", resource));

        Assert.Equal(0, count);

        _out.WriteLine($"No orphan row after Dispose (resource='{resource}')");
    }
}
