using System;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

/// <summary>
/// Verifies that each gateway's DeleteExpiredAsync removes rows whose ExpireAt is in the past
/// and leaves rows that have no expiry or a future expiry untouched.
/// </summary>
public abstract class ExpirationManagerFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected ExpirationManagerFacts(TFixture fixture) => _f = fixture;

    // ── Job ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteExpired_Job_RemovesExpiredRows()
    {
        var id = await _f.InsertJobAsync(expireAt: DateTime.UtcNow.AddHours(-1));

        await _f.Storage.Jobs.DeleteExpiredAsync(1000);

        var job = await _f.Storage.Jobs.RetrieveOneAsync(id);
        Assert.Null(job);
    }

    [Fact]
    public async Task DeleteExpired_Job_LeavesUnexpiredRows()
    {
        var id = await _f.InsertJobAsync(expireAt: DateTime.UtcNow.AddHours(24));

        await _f.Storage.Jobs.DeleteExpiredAsync(1000);

        var job = await _f.Storage.Jobs.RetrieveOneAsync(id);
        Assert.NotNull(job);
    }

    [Fact]
    public async Task DeleteExpired_Job_LeavesRowsWithNoExpiration()
    {
        var id = await _f.InsertJobAsync();

        await _f.Storage.Jobs.DeleteExpiredAsync(1000);

        var job = await _f.Storage.Jobs.RetrieveOneAsync(id);
        Assert.NotNull(job);
    }

    // ── Hash ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteExpired_Hash_RemovesExpiredRows()
    {
        var key = "exp-hash-del-" + Guid.NewGuid();
        await _f.InsertHashAsync(key, "f1", expireAt: DateTime.UtcNow.AddHours(-1));

        await _f.Storage.Hashes.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task DeleteExpired_Hash_LeavesUnexpiredRows()
    {
        var key = "exp-hash-keep-" + Guid.NewGuid();
        await _f.InsertHashAsync(key, "f1", expireAt: DateTime.UtcNow.AddHours(24));

        await _f.Storage.Hashes.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
    }

    // ── Set ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteExpired_Set_RemovesExpiredRows()
    {
        var key = "exp-set-del-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "v1", expireAt: DateTime.UtcNow.AddHours(-1));

        await _f.Storage.Sets.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task DeleteExpired_Set_LeavesUnexpiredRows()
    {
        var key = "exp-set-keep-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "v1", expireAt: DateTime.UtcNow.AddHours(24));

        await _f.Storage.Sets.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
    }

    // ── List ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteExpired_List_RemovesExpiredRows()
    {
        var key = "exp-list-del-" + Guid.NewGuid();
        await _f.InsertListAsync(key, expireAt: DateTime.UtcNow.AddHours(-1));

        await _f.Storage.Lists.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task DeleteExpired_List_LeavesUnexpiredRows()
    {
        var key = "exp-list-keep-" + Guid.NewGuid();
        await _f.InsertListAsync(key, expireAt: DateTime.UtcNow.AddHours(24));

        await _f.Storage.Lists.DeleteExpiredAsync(1000);

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
    }

    // ── AggregatedCounter ────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteExpired_AggregatedCounter_RemovesExpiredRows()
    {
        var key = "exp-aggcnt-" + Guid.NewGuid();
        await _f.Storage.AggregatedCounters.CreateAsync(new AggregatedCounter {
            Key = key,
            Value = 1,
            ExpireAt = DateTime.UtcNow.AddHours(-1)
        });

        await _f.Storage.AggregatedCounters.DeleteExpiredAsync(1000);

        var row = await _f.Storage.AggregatedCounters.RetrieveOneAsync(key);
        Assert.Null(row);
    }
}

[Collection("Sqlite")]
public class SqliteExpirationManagerFacts : ExpirationManagerFacts<SqliteFixture>
{
    public SqliteExpirationManagerFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresExpirationManagerFacts : ExpirationManagerFacts<PostgresFixture>
{
    public PostgresExpirationManagerFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerExpirationManagerFacts : ExpirationManagerFacts<SqlServerFixture>
{
    public SqlServerExpirationManagerFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlExpirationManagerFacts : ExpirationManagerFacts<MySqlFixture>
{
    public MySqlExpirationManagerFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleExpirationManagerFacts : ExpirationManagerFacts<OracleFixture>
{
    public OracleExpirationManagerFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdExpirationManagerFacts : ExpirationManagerFacts<FirebirdFixture>
{
    public FirebirdExpirationManagerFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbExpirationManagerFacts : ExpirationManagerFacts<CockroachDbFixture>
{
    public CockroachDbExpirationManagerFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbExpirationManagerFacts : ExpirationManagerFacts<MariaDbFixture>
{
    public MariaDbExpirationManagerFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbExpirationManagerFacts : ExpirationManagerFacts<DuckDbFixture>
{
    public DuckDbExpirationManagerFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbExpirationManagerFacts : ExpirationManagerFacts<YugabyteDbFixture>
{
    public YugabyteDbExpirationManagerFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbExpirationManagerFacts : ExpirationManagerFacts<TiDbFixture>
{
    public TiDbExpirationManagerFacts(TiDbFixture fixture) : base(fixture) { }
}
