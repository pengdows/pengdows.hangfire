using System;
using System.Threading.Tasks;
using Xunit;

namespace pengdows.hangfire.integration.tests;

public abstract class FetchedJobWatchdogFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected FetchedJobWatchdogFacts(TFixture fixture) => _f = fixture;

    private (PengdowsCrudJobStorage Storage, FetchedJobWatchdog Watchdog) CreateWatchdog(TimeSpan invisibilityTimeout)
    {
        var opts = new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema   = false,
            InvisibilityTimeout = invisibilityTimeout
        };
        var storage  = new PengdowsCrudJobStorage(_f.Context, opts);
        var watchdog = new FetchedJobWatchdog(storage, invisibilityTimeout);
        return (storage, watchdog);
    }

    [Fact]
    public async Task RunOnce_RequeuesStaleJob_UsingInvisibilityTimeout()
    {
        var invisTimeout = TimeSpan.FromMinutes(5);
        var (storage, watchdog) = CreateWatchdog(invisTimeout);

        // Fetched well outside the invisibility window — should be requeued
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "stale-wdg",
            fetchedAt: DateTime.UtcNow - invisTimeout - TimeSpan.FromMinutes(1));

        watchdog.RunOnce();

        var rows = await storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq   = Assert.Single(rows);
        Assert.Null(jq.FetchedAt);
    }

    [Fact]
    public async Task RunOnce_DoesNotRequeue_RecentlyFetchedJob()
    {
        var invisTimeout = TimeSpan.FromMinutes(5);
        var (storage, watchdog) = CreateWatchdog(invisTimeout);

        // Fetched well within the invisibility window — must stay claimed
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "active-wdg",
            fetchedAt: DateTime.UtcNow - TimeSpan.FromSeconds(30));

        watchdog.RunOnce();

        var rows = await storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq   = Assert.Single(rows);
        Assert.NotNull(jq.FetchedAt);
    }
}

[Collection("Sqlite")]
public class SqliteFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<SqliteFixture>
{
    public SqliteFetchedJobWatchdogFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<PostgresFixture>
{
    public PostgresFetchedJobWatchdogFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<SqlServerFixture>
{
    public SqlServerFetchedJobWatchdogFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<MySqlFixture>
{
    public MySqlFetchedJobWatchdogFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<OracleFixture>
{
    public OracleFetchedJobWatchdogFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<FirebirdFixture>
{
    public FirebirdFetchedJobWatchdogFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<CockroachDbFixture>
{
    public CockroachDbFetchedJobWatchdogFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<MariaDbFixture>
{
    public MariaDbFetchedJobWatchdogFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<DuckDbFixture>
{
    public DuckDbFetchedJobWatchdogFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<YugabyteDbFixture>
{
    public YugabyteDbFetchedJobWatchdogFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbFetchedJobWatchdogFacts : FetchedJobWatchdogFacts<TiDbFixture>
{
    public TiDbFetchedJobWatchdogFacts(TiDbFixture fixture) : base(fixture) { }
}
