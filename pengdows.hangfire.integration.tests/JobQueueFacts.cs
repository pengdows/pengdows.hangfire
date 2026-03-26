using System.Threading;
using System.Threading.Tasks;
using System;
using System.Linq;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

public abstract class JobQueueFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected JobQueueFacts(TFixture fixture) => _f = fixture;

    [Fact]
    public async Task FetchNextJob_ReturnsNull_WhenQueueIsEmpty()
    {
        var result = await _f.Storage.JobQueues.FetchNextJobAsync(
            ["emptyqueue"], CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task FetchNextJob_ReturnsJobId_WhenJobExists()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "testqueue");

        var result = await _f.Storage.JobQueues.FetchNextJobAsync(
            ["testqueue"], CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(jobId, result!.Value.JobId);
        Assert.Equal("testqueue", result.Value.Queue);
    }

    [Fact]
    public async Task FetchNextJob_SetsFetchedAt_OnClaim()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "claimqueue");

        await _f.Storage.JobQueues.FetchNextJobAsync(["claimqueue"], CancellationToken.None);

        var rows = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq = Assert.Single(rows);
        Assert.NotNull(jq.FetchedAt);
    }

    [Fact]
    public async Task FetchNextJob_SkipsAlreadyFetchedJobs()
    {
        var jobId1 = await _f.InsertJobAsync();
        var jobId2 = await _f.InsertJobAsync();
        // jobId1 is already fetched
        await _f.InsertJobQueueAsync(jobId1, "skipqueue", fetchedAt: DateTime.UtcNow);
        await _f.InsertJobQueueAsync(jobId2, "skipqueue");

        var result = await _f.Storage.JobQueues.FetchNextJobAsync(
            ["skipqueue"], CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(jobId2, result!.Value.JobId);
    }

    [Fact]
    public async Task AcknowledgeAsync_DeletesQueueRow()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "ackqueue", fetchedAt: DateTime.UtcNow);

        var rows = await _f.Storage.JobQueues.AcknowledgeAsync(jobId, "ackqueue");
        Assert.Equal(1, rows);

        var results = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        Assert.Empty(results);
    }

    [Fact]
    public async Task RequeueAsync_SetsFetchedAtToNull()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "reqqueue", fetchedAt: DateTime.UtcNow);

        var rows = await _f.Storage.JobQueues.RequeueAsync(jobId, "reqqueue");
        Assert.Equal(1, rows);

        var results = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq = Assert.Single(results);
        Assert.Null(jq.FetchedAt);
    }

    [Fact]
    public async Task GetDistinctQueues_ReturnsUniqueQueues()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "qa");
        await _f.InsertJobQueueAsync(jobId, "qa");
        await _f.InsertJobQueueAsync(jobId, "qb");

        var queues = await _f.Storage.JobQueues.GetDistinctQueuesAsync();
        Assert.Contains("qa", queues);
        Assert.Contains("qb", queues);
        Assert.Equal(queues.Distinct().Count(), queues.Count);
    }
}

[Collection("Sqlite")]
public class SqliteJobQueueFacts : JobQueueFacts<SqliteFixture>
{
    public SqliteJobQueueFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresJobQueueFacts : JobQueueFacts<PostgresFixture>
{
    public PostgresJobQueueFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerJobQueueFacts : JobQueueFacts<SqlServerFixture>
{
    public SqlServerJobQueueFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleJobQueueFacts : JobQueueFacts<OracleFixture>
{
    public OracleJobQueueFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdJobQueueFacts : JobQueueFacts<FirebirdFixture>
{
    public FirebirdJobQueueFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbJobQueueFacts : JobQueueFacts<CockroachDbFixture>
{
    public CockroachDbJobQueueFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbJobQueueFacts : JobQueueFacts<MariaDbFixture>
{
    public MariaDbJobQueueFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbJobQueueFacts : JobQueueFacts<DuckDbFixture>
{
    public DuckDbJobQueueFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbJobQueueFacts : JobQueueFacts<YugabyteDbFixture>
{
    public YugabyteDbJobQueueFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbJobQueueFacts : JobQueueFacts<TiDbFixture>
{
    public TiDbJobQueueFacts(TiDbFixture fixture) : base(fixture) { }
}

