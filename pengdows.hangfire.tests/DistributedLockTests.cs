using System;
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
    public void Constructor_AcquiresLock_InsertWinMode()
    {
        var (storage, _) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "test-resource", TimeSpan.FromSeconds(30));
        Assert.Equal(AcquireMode.InsertWin, lk.HowAcquired);
        Assert.Equal(0, lk.AcquireRetryCount);
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
    public void AcquireSleepMs_ZeroOnFirstAcquire()
    {
        var (storage, _) = CreateStorage();
        using var lk = new PengdowsCrudDistributedLock(storage, "res-sleep", TimeSpan.FromSeconds(30));
        Assert.Equal(0L, lk.AcquireSleepMs);
    }

    [Fact]
    public void Constructor_WithJitterDisabled_AcquiresLock()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions { DistributedLockRetryJitter = false };
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        using var lk = new PengdowsCrudDistributedLock(storage, "res-nojitter", TimeSpan.FromSeconds(30));
        Assert.Equal(AcquireMode.InsertWin, lk.HowAcquired);
    }

    [Fact]
    public void Constructor_WhenInsertConflictsAndLockStealSucceeds_AcquiresTtlSteal()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);

        // conn2: UPDATE (TryUpdateExpiredAsync) returns 1 → stolen=true
        var conn2 = new fakeDbConnection();
        conn2.NonQueryResults.Enqueue(1);
        factory.Connections.Insert(0, conn2);

        // conn1: INSERT (CreateAsync) throws UniqueConstraintViolationException
        var conn1 = new fakeDbConnection();
        conn1.SetNonQueryExecuteException(
            new UniqueConstraintViolationException("dup", SupportedDatabase.SqlServer));
        factory.Connections.Insert(0, conn1);

        using var lk = new PengdowsCrudDistributedLock(storage, "res-steal", TimeSpan.FromSeconds(30));
        Assert.Equal(AcquireMode.TtlSteal, lk.HowAcquired);
    }

    [Fact]
    public void Constructor_WhenInsertConflictsAndNoExpiredLock_ThrowsTimeout()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions { DistributedLockRetryDelay = TimeSpan.FromMilliseconds(1) };
        var storage = new PengdowsCrudJobStorage(ctx, opts);

        // conn2: UPDATE returns 0 → stolen=false → (null, false)
        var conn2 = new fakeDbConnection();
        conn2.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn2);

        // conn1: INSERT throws UniqueConstraintViolationException
        var conn1 = new fakeDbConnection();
        conn1.SetNonQueryExecuteException(
            new UniqueConstraintViolationException("dup", SupportedDatabase.SqlServer));
        factory.Connections.Insert(0, conn1);

        // timeout=Zero → deadline is already in the past → DistributedLockTimeoutException
        Assert.Throws<DistributedLockTimeoutException>(() =>
            new PengdowsCrudDistributedLock(storage, "res-timeout", TimeSpan.Zero));
    }

    [Fact]
    public void Constructor_WhenInsertConflictsThenRetrySucceeds_AcquiresFollowRelease()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions {
            DistributedLockRetryDelay  = TimeSpan.FromMilliseconds(1),
            DistributedLockRetryJitter = true
        };
        var storage = new PengdowsCrudJobStorage(ctx, opts);

        // conn2: UPDATE returns 0 → stolen=false → retry loop entered
        var conn2 = new fakeDbConnection();
        conn2.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn2);

        // conn1: INSERT throws UniqueConstraintViolationException (first attempt)
        var conn1 = new fakeDbConnection();
        conn1.SetNonQueryExecuteException(
            new UniqueConstraintViolationException("dup", SupportedDatabase.SqlServer));
        factory.Connections.Insert(0, conn1);

        // long timeout: retry happens; second INSERT (conn3, auto-created) succeeds
        using var lk = new PengdowsCrudDistributedLock(storage, "res-retry", TimeSpan.FromSeconds(30));
        Assert.Equal(AcquireMode.FollowRelease, lk.HowAcquired);
        Assert.Equal(1, lk.AcquireRetryCount);
    }

    [Fact]
    public void Constructor_WhenInsertConflictsThenRetryNoJitter_AcquiresFollowRelease()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions {
            DistributedLockRetryDelay  = TimeSpan.FromMilliseconds(1),
            DistributedLockRetryJitter = false
        };
        var storage = new PengdowsCrudJobStorage(ctx, opts);

        var conn2 = new fakeDbConnection();
        conn2.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, conn2);

        var conn1 = new fakeDbConnection();
        conn1.SetNonQueryExecuteException(
            new UniqueConstraintViolationException("dup", SupportedDatabase.SqlServer));
        factory.Connections.Insert(0, conn1);

        using var lk = new PengdowsCrudDistributedLock(storage, "res-retry-nojitter", TimeSpan.FromSeconds(30));
        Assert.Equal(AcquireMode.FollowRelease, lk.HowAcquired);
    }
}
