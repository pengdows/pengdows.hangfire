using System;
using System.Threading.Tasks;
using Hangfire.Storage;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

public abstract class DistributedLockFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected DistributedLockFacts(TFixture fixture) => _f = fixture;

    [Fact]
    public void AcquireLock_ReturnsInstance()
    {
        using var lk = new PengdowsCrudDistributedLock(_f.Storage, "lock-acquire-" + Guid.NewGuid(), TimeSpan.FromSeconds(5));
        Assert.NotNull(lk);
    }

    [Fact]
    public async Task AcquireLock_WritesLockRow()
    {
        var resource = "lock-row-" + Guid.NewGuid();
        using var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));

        var record = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record);
        Assert.False(string.IsNullOrEmpty(record.OwnerId));
        Assert.True(record.ExpiresAt > DateTime.UtcNow);
    }

    [Fact]
    public async Task Dispose_ReleasesLockRow()
    {
        var resource = "lock-dispose-" + Guid.NewGuid();
        var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));
        lk.Dispose();

        var record = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.Null(record);
    }

    [Fact]
    public void AcquireLock_Throws_WhenLockAlreadyHeld()
    {
        var resource = "lock-timeout-" + Guid.NewGuid();
        using var first = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));

        Assert.Throws<DistributedLockTimeoutException>(() =>
            new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.Zero));
    }

    [Fact]
    public void DoubleDispose_IsIdempotent()
    {
        var resource = "lock-dbl-disp-" + Guid.NewGuid();
        var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));
        lk.Dispose();
        lk.Dispose(); // must not throw
    }

    [Fact]
    public async Task ExpiredLock_CanBeStolen()
    {
        var resource = "lock-steal-" + Guid.NewGuid();

        // Insert a lock row that has already expired
        await _f.Storage.Locks.CreateAsync(new DistributedLockRecord {
            Resource = resource,
            OwnerId = "old-owner",
            ExpiresAt = DateTime.UtcNow.AddMinutes(-1),
            Version = 1
        });

        // New acquire should steal the expired row and succeed without timeout
        using var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));
        Assert.NotNull(lk);
        Assert.Equal(AcquireMode.TtlSteal, lk.HowAcquired);

        var record = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record);
        Assert.False(string.IsNullOrWhiteSpace(record.OwnerId));
        Assert.NotEqual("old-owner", record.OwnerId);
    }

    [Fact]
    public async Task HeldLock_TimesOut_AndPreservesOriginalOwner()
    {
        var resource = "lock-held-" + Guid.NewGuid();

        await _f.Storage.Locks.CreateAsync(new DistributedLockRecord {
            Resource = resource,
            OwnerId = "current-owner",
            ExpiresAt = DateTime.UtcNow.AddMinutes(5),
            Version = 1
        });

        Assert.Throws<DistributedLockTimeoutException>(() =>
            new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.Zero));

        var record = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record);
        Assert.Equal("current-owner", record.OwnerId);
    }

    [Fact]
    public async Task Heartbeat_RenewsExpiresAt()
    {
        var resource = "lock-heartbeat-" + Guid.NewGuid();

        // Use a short TTL so the heartbeat fires quickly (TTL/5 = 1s)
        var opts = new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema  = false,
            DistributedLockTtl = TimeSpan.FromSeconds(5)
        };
        var storage = new PengdowsCrudJobStorage(_f.Context, opts);

        using var lk = new PengdowsCrudDistributedLock(storage, resource, TimeSpan.FromSeconds(10));

        var record1 = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record1);
        var firstExpiry = record1.ExpiresAt;

        // Wait for one heartbeat tick (TTL/5 + buffer = 1s + 500ms)
        await Task.Delay(TimeSpan.FromMilliseconds(1500));

        var record2 = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record2);
        Assert.True(record2.ExpiresAt > firstExpiry);
    }

    [Fact]
    public async Task TryRenew_FailsOnVersionMismatch()
    {
        var resource = "lock-version-" + Guid.NewGuid();
        using var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));

        var renewed = await _f.Storage.Locks.TryRenewAsync(
            resource, "wrong-owner", 999, DateTime.UtcNow.AddMinutes(5));

        Assert.False(renewed);
    }

    [Fact]
    public async Task Release_DoesNotDelete_RowOwnedByOther()
    {
        var resource = "lock-owner-guard-" + Guid.NewGuid();

        // Insert a row owned by owner A
        await _f.Storage.Locks.CreateAsync(new DistributedLockRecord {
            Resource = resource,
            OwnerId = "owner-a",
            ExpiresAt = DateTime.UtcNow.AddMinutes(5),
            Version = 1
        });

        // Attempt release as owner B
        await _f.Storage.Locks.ReleaseAsync(resource, "owner-b");

        // Owner A's row must still exist
        var record = await _f.Storage.Locks.RetrieveOneAsync(resource);
        Assert.NotNull(record);
        Assert.Equal("owner-a", record.OwnerId);
    }
}

[Collection("Sqlite")]
public class SqliteDistributedLockFacts : DistributedLockFacts<SqliteFixture>
{
    public SqliteDistributedLockFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresDistributedLockFacts : DistributedLockFacts<PostgresFixture>
{
    public PostgresDistributedLockFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerDistributedLockFacts : DistributedLockFacts<SqlServerFixture>
{
    public SqlServerDistributedLockFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlDistributedLockFacts : DistributedLockFacts<MySqlFixture>
{
    public MySqlDistributedLockFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleDistributedLockFacts : DistributedLockFacts<OracleFixture>
{
    public OracleDistributedLockFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdDistributedLockFacts : DistributedLockFacts<FirebirdFixture>
{
    public FirebirdDistributedLockFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbDistributedLockFacts : DistributedLockFacts<CockroachDbFixture>
{
    public CockroachDbDistributedLockFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbDistributedLockFacts : DistributedLockFacts<MariaDbFixture>
{
    public MariaDbDistributedLockFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbDistributedLockFacts : DistributedLockFacts<DuckDbFixture>
{
    public DuckDbDistributedLockFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbDistributedLockFacts : DistributedLockFacts<YugabyteDbFixture>
{
    public YugabyteDbDistributedLockFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbDistributedLockFacts : DistributedLockFacts<TiDbFixture>
{
    public TiDbDistributedLockFacts(TiDbFixture fixture) : base(fixture) { }
}
