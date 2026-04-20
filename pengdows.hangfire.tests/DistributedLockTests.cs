using System;
using System.Reflection;
using System.Threading.Tasks;
using Hangfire.Storage;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.exceptions;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class DistributedLockTests
{
    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    [Fact]
    public void Constructor_AcquiresLock_Successfully()
    {
        var (storage, _) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "test-resource", TimeSpan.FromSeconds(30));
        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new PengdowsCrudDistributedLock(null!, "res", TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void Constructor_NullResource_Throws()
    {
        var (storage, _) = CreateStorage();
        Assert.Throws<ArgumentNullException>(() =>
            new PengdowsCrudDistributedLock(storage, null!, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public void Dispose_ReleasesLock()
    {
        var (storage, _) = CreateStorage();
        var lk = new PengdowsCrudDistributedLock(storage, "res-dispose", TimeSpan.FromSeconds(30));
        lk.Dispose(); // Should not throw
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        var (storage, _) = CreateStorage();
        var lk = new PengdowsCrudDistributedLock(storage, "res-double-dispose", TimeSpan.FromSeconds(30));
        lk.Dispose();
        lk.Dispose(); // Second dispose should be a no-op
    }

    [Fact]
    public void LeaseLost_InitiallyFalse()
    {
        var (storage, _) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "res-lease", TimeSpan.FromSeconds(30));
        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public void Constructor_WhenUpsertReturnsZero_ThrowsTimeout()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);

        // Seed a connection that returns 0 from ExecuteNonQueryAsync (UPSERT acquired 0 rows — lock held)
        var conn = new fakeDbConnection();
        conn.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn);

        Assert.Throws<DistributedLockTimeoutException>(() =>
            new PengdowsCrudDistributedLock(storage, "res-timeout", TimeSpan.Zero));
    }

    [Fact]
    public void Constructor_WhenUpsertReturnsOne_Succeeds()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);

        // fakeDb returns 1 by default for NonQuery — UPSERT succeeds
        using var lk = new PengdowsCrudDistributedLock(storage, "res-upsert-win", TimeSpan.FromSeconds(30));
        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public void JitteredDelay_ReturnsValueInExpectedRange()
    {
        // JitteredDelay: ms = base/2 + NextInt64(base) → range [base/2, base*3/2)
        // For base=100ms: [50ms, 149ms] inclusive
        var method = typeof(PengdowsCrudDistributedLock)
            .GetMethod("JitteredDelay", BindingFlags.NonPublic | BindingFlags.Static)!;

        var baseDelay = TimeSpan.FromMilliseconds(100);
        for (var i = 0; i < 20; i++)
        {
            var result = (TimeSpan)method.Invoke(null, [baseDelay])!;
            Assert.InRange(result.TotalMilliseconds, 50.0, 149.0);
        }
    }

    [Fact]
    public void Constructor_JitterDisabled_AcquiresLockAfterRetry()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions
        {
            DistributedLockRetryJitter = false,
            DistributedLockRetryDelay = TimeSpan.FromMilliseconds(1)
        };
        var storage = new PengdowsCrudJobStorage(ctx, opts);

        // First TryAcquireAsync returns 0 (lock contended); on retry the default 1 succeeds
        var conn = new fakeDbConnection();
        conn.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn);

        using var lk = new PengdowsCrudDistributedLock(storage, "jitter-disabled", TimeSpan.FromSeconds(5));
        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public async Task RenewAsync_WhenRenewalReturnsFalse_SetsLeaseLost()
    {
        var (storage, factory) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "renew-fail", TimeSpan.FromSeconds(30));
        Assert.False(lk.LeaseLost);

        // Inject a connection returning 0 for the next non-query so TryRenewAsync returns false
        // (each CreateSqlContainer opens a fresh connection from the factory queue)
        var conn = new fakeDbConnection();
        conn.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn);

        var renewMethod = typeof(PengdowsCrudDistributedLock)
            .GetMethod("RenewAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)renewMethod.Invoke(lk, null)!;

        Assert.True(lk.LeaseLost);
    }

    [Fact]
    public async Task RenewAsync_WhenRenewalSucceeds_DoesNotSetLeaseLost()
    {
        var (storage, _) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "renew-success", TimeSpan.FromSeconds(30));

        // Default fakeDb returns 1 for NonQuery → TryRenewAsync returns true → success path
        var renewMethod = typeof(PengdowsCrudDistributedLock)
            .GetMethod("RenewAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)renewMethod.Invoke(lk, null)!;

        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public async Task RenewAsync_WhenTryRenewThrows_DoesNotPropagate()
    {
        var (storage, factory) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "renew-throw", TimeSpan.FromSeconds(30));

        var conn = new fakeDbConnection();
        conn.SetNonQueryExecuteException(new Exception("db error"));
        factory.Connections.Insert(0, conn);

        var renewMethod = typeof(PengdowsCrudDistributedLock)
            .GetMethod("RenewAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)renewMethod.Invoke(lk, null)!;  // must not throw

        Assert.False(lk.LeaseLost);
    }

    [Fact]
    public void Dispose_WhenReleaseFails_DoesNotThrow()
    {
        var (storage, factory) = CreateStorage();
        var lk = new PengdowsCrudDistributedLock(storage, "dispose-fail", TimeSpan.FromSeconds(30));

        var conn = new fakeDbConnection();
        conn.SetNonQueryExecuteException(new Exception("release failed"));
        factory.Connections.Insert(0, conn);

        lk.Dispose();  // must not throw even when ReleaseAsync fails
    }
}
