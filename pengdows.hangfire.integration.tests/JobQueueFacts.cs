using System.Threading;
using System.Threading.Tasks;
using System;
using System.Linq;
using pengdows.hangfire;
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
        Assert.False(string.IsNullOrWhiteSpace(jq.FetchToken));
    }

    [Fact]
    public async Task StaleClaim_CannotAcknowledgeOrRequeueNewerClaim()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "fencedqueue");

        var first = await _f.Storage.JobQueues.FetchNextJobAsync(["fencedqueue"], CancellationToken.None);
        Assert.NotNull(first);

        await _f.Storage.JobQueues.RequeueAsync(jobId, "fencedqueue", first!.Value.FetchToken);
        var second = await _f.Storage.JobQueues.FetchNextJobAsync(["fencedqueue"], CancellationToken.None);
        Assert.NotNull(second);
        Assert.NotEqual(first.Value.FetchToken, second!.Value.FetchToken);

        Assert.Equal(0, await _f.Storage.JobQueues.AcknowledgeAsync(jobId, "fencedqueue", first.Value.FetchToken));
        Assert.Equal(0, await _f.Storage.JobQueues.RequeueAsync(jobId, "fencedqueue", first.Value.FetchToken));

        var row = Assert.Single(await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId));
        Assert.Equal(second.Value.FetchToken, row.FetchToken);
        Assert.NotNull(row.FetchedAt);

        await _f.Storage.JobQueues.RequeueAsync(jobId, "fencedqueue", second.Value.FetchToken);
    }

    [Fact]
    public async Task FetchedJob_RefreshesFetchedAtWhileRunning()
    {
        _f.Storage.Options.InvisibilityTimeout = TimeSpan.FromSeconds(3);
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "heartbeatqueue");
        var claim = await _f.Storage.JobQueues.FetchNextJobAsync(["heartbeatqueue"], CancellationToken.None);
        Assert.NotNull(claim);

        using var fetched = new PengdowsCrudFetchedJob(
            _f.Storage, jobId, "heartbeatqueue", claim!.Value.FetchToken);
        var initial = Assert.Single(await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId));
        await Task.Delay(TimeSpan.FromMilliseconds(1400));
        var refreshed = Assert.Single(await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId));

        Assert.True(refreshed.FetchedAt > initial.FetchedAt,
            $"Expected heartbeat to advance FetchedAt; initial={initial.FetchedAt:o}, refreshed={refreshed.FetchedAt:o}");
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
    public async Task RequeueStaleAsync_RequeuesExpiredFetchedJobs()
    {
        var jobId = await _f.InsertJobAsync();
        // Insert a row fetched well beyond the invisibility timeout
        await _f.InsertJobQueueAsync(jobId, "stalequeue", fetchedAt: DateTime.UtcNow.AddHours(-2));

        var cutoff = DateTime.UtcNow.AddMinutes(-5);
        var affected = await _f.Storage.JobQueues.RequeueStaleAsync(cutoff);
        Assert.Equal(1, affected);

        var rows = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq = Assert.Single(rows);
        Assert.Null(jq.FetchedAt);
    }

    [Fact]
    public async Task RequeueStaleAsync_DoesNotRequeue_RecentlyFetchedJobs()
    {
        var jobId = await _f.InsertJobAsync();
        // Insert a row fetched only 30 seconds ago — well within the timeout
        var recentFetch = DateTime.UtcNow.AddSeconds(-30);
        await _f.InsertJobQueueAsync(jobId, "activequeue", fetchedAt: recentFetch);

        var cutoff = DateTime.UtcNow.AddMinutes(-5);
        var affected = await _f.Storage.JobQueues.RequeueStaleAsync(cutoff);
        Assert.Equal(0, affected);

        var rows = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq = Assert.Single(rows);
        Assert.NotNull(jq.FetchedAt);
    }

    [Fact]
    public async Task RequeueStaleAsync_DoesNotRequeue_UnfetchedJobs()
    {
        var jobId = await _f.InsertJobAsync();
        await _f.InsertJobQueueAsync(jobId, "unfetchedqueue");

        var cutoff = DateTime.UtcNow.AddMinutes(-5);
        var affected = await _f.Storage.JobQueues.RequeueStaleAsync(cutoff);
        Assert.Equal(0, affected);

        var rows = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        var jq = Assert.Single(rows);
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
