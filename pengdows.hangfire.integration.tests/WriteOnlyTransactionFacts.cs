using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.States;
using Hangfire.Storage;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

public abstract class WriteOnlyTransactionFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected WriteOnlyTransactionFacts(TFixture fixture) => _f = fixture;

    // ── helpers ───────────────────────────────────────────────────────────────

    private async Task CommitAsync(Action<PengdowsCrudWriteOnlyTransaction> action)
    {
        using var tx = new PengdowsCrudWriteOnlyTransaction(_f.Storage);
        action(tx);
        await tx.CommitAsync();
    }

    // ── ExpireJob ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task ExpireJob_SetsJobExpirationDate()
    {
        var jobId = await _f.InsertJobAsync();
        var expireAt = DateTime.UtcNow.AddHours(24);

        await CommitAsync(tx => tx.ExpireJob(jobId.ToString(), TimeSpan.FromHours(24)));

        var job = await _f.Storage.Jobs.RetrieveOneAsync(jobId);
        Assert.NotNull(job);
        Assert.NotNull(job.ExpireAt);
        Assert.True(job.ExpireAt.Value > DateTime.UtcNow.AddHours(23));
    }

    [Fact]
    public async Task ExpireJob_DoesNotAffectOtherJobs()
    {
        var jobId1 = await _f.InsertJobAsync();
        var jobId2 = await _f.InsertJobAsync();

        await CommitAsync(tx => tx.ExpireJob(jobId1.ToString(), TimeSpan.FromHours(24)));

        var job2 = await _f.Storage.Jobs.RetrieveOneAsync(jobId2);
        Assert.NotNull(job2);
        Assert.Null(job2.ExpireAt);
    }

    // ── PersistJob ────────────────────────────────────────────────────────────

    [Fact]
    public async Task PersistJob_ClearsExpirationDate()
    {
        var jobId = await _f.InsertJobAsync(expireAt: DateTime.UtcNow.AddHours(1));

        await CommitAsync(tx => tx.PersistJob(jobId.ToString()));

        var job = await _f.Storage.Jobs.RetrieveOneAsync(jobId);
        Assert.NotNull(job);
        Assert.Null(job.ExpireAt);
    }

    // ── SetJobState ───────────────────────────────────────────────────────────

    [Fact]
    public async Task SetJobState_InsertsStateRow()
    {
        var jobId = await _f.InsertJobAsync();

        await CommitAsync(tx => tx.SetJobState(jobId.ToString(), new SucceededState(null, 1, 100)));

        var states = await _f.Storage.JobStates.GetWhereAsync("JobId", jobId);
        Assert.Contains(states, s => s.Name == "Succeeded");
    }

    [Fact]
    public async Task SetJobState_UpdatesJobStateName()
    {
        var jobId = await _f.InsertJobAsync();

        await CommitAsync(tx => tx.SetJobState(jobId.ToString(), new SucceededState(null, 1, 100)));

        var job = await _f.Storage.Jobs.RetrieveOneAsync(jobId);
        Assert.NotNull(job);
        Assert.Equal("Succeeded", job.StateName);
    }

    // ── AddJobState ───────────────────────────────────────────────────────────

    [Fact]
    public async Task AddJobState_InsertsStateWithoutChangingJobStateName()
    {
        var jobId = await _f.InsertJobAsync(stateName: "Processing");

        await CommitAsync(tx => tx.AddJobState(jobId.ToString(), new FailedState(new Exception("boom"))));

        var job = await _f.Storage.Jobs.RetrieveOneAsync(jobId);
        Assert.NotNull(job);
        Assert.Equal("Processing", job.StateName); // unchanged

        var states = await _f.Storage.JobStates.GetWhereAsync("JobId", jobId);
        Assert.Contains(states, s => s.Name == "Failed");
    }

    // ── AddToQueue ────────────────────────────────────────────────────────────

    [Fact]
    public async Task AddToQueue_InsertsJobQueueRow()
    {
        var jobId = await _f.InsertJobAsync();

        await CommitAsync(tx => tx.AddToQueue("default", jobId.ToString()));

        var queues = await _f.Storage.JobQueues.GetWhereAsync("JobId", jobId);
        Assert.Contains(queues, q => q.Queue == "default");
    }

    // ── IncrementCounter / DecrementCounter ───────────────────────────────────

    [Fact]
    public async Task IncrementCounter_InsertsRowWithPositiveDelta()
    {
        await CommitAsync(tx => tx.IncrementCounter("stats:succeeded"));

        var rows = await _f.Storage.Counters.GetWhereAsync("Key", "stats:succeeded");
        Assert.NotEmpty(rows);
        Assert.Contains(rows, r => r.Value > 0);
    }

    [Fact]
    public async Task DecrementCounter_InsertsRowWithNegativeDelta()
    {
        await CommitAsync(tx => tx.DecrementCounter("stats:failed"));

        var rows = await _f.Storage.Counters.GetWhereAsync("Key", "stats:failed");
        Assert.NotEmpty(rows);
        Assert.Contains(rows, r => r.Value < 0);
    }

    // ── SetRangeInHash ────────────────────────────────────────────────────────

    [Fact]
    public async Task SetRangeInHash_InsertsFieldValuePairs()
    {
        var key = "job:1:params-" + Guid.NewGuid();
        await CommitAsync(tx => tx.SetRangeInHash(key,
            new Dictionary<string, string> { ["CurrentCulture"] = "en-US", ["Param"] = "42" }));

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.Field == "CurrentCulture" && r.Value == "en-US");
    }

    // ── AddToSet / RemoveFromSet ──────────────────────────────────────────────

    [Fact]
    public async Task AddToSet_InsertsRow()
    {
        var key = "recurring-jobs-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "job-1"));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Contains(rows, r => r.Value == "job-1");
    }

    [Fact]
    public async Task RemoveFromSet_DeletesRow()
    {
        var key = "recurring-jobs-del-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "job-del"));
        await CommitAsync(tx => tx.RemoveFromSet(key, "job-del"));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.DoesNotContain(rows, r => r.Value == "job-del");
    }

    // ── InsertToList / RemoveFromList ─────────────────────────────────────────

    [Fact]
    public async Task InsertToList_AddsItem()
    {
        var key = "mylist-" + Guid.NewGuid();
        await CommitAsync(tx => tx.InsertToList(key, "item1"));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
        Assert.Contains(rows, r => r.Value == "item1");
    }

    // ── ExpireSet / PersistSet ────────────────────────────────────────────────

    [Fact]
    public async Task ExpireSet_SetsExpireAt()
    {
        var key = "expset-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "v"));
        await CommitAsync(tx => tx.ExpireSet(key, TimeSpan.FromHours(1)));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.NotNull(rows[0].ExpireAt);
    }

    [Fact]
    public async Task PersistSet_ClearsExpireAt()
    {
        var key = "persset-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "v"));
        await CommitAsync(tx => tx.ExpireSet(key, TimeSpan.FromHours(1)));
        await CommitAsync(tx => tx.PersistSet(key));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.Null(rows[0].ExpireAt);
    }

    // ── ExpireHash / PersistHash ──────────────────────────────────────────────

    [Fact]
    public async Task ExpireHash_SetsExpireAt()
    {
        var key = "exphash-" + Guid.NewGuid();
        await CommitAsync(tx => tx.SetRangeInHash(key, new Dictionary<string, string> { ["f"] = "v" }));
        await CommitAsync(tx => tx.ExpireHash(key, TimeSpan.FromHours(1)));

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.NotNull(rows[0].ExpireAt);
    }

    [Fact]
    public async Task PersistHash_ClearsExpireAt()
    {
        var key = "pershash-" + Guid.NewGuid();
        await CommitAsync(tx => tx.SetRangeInHash(key, new Dictionary<string, string> { ["f"] = "v" }));
        await CommitAsync(tx => tx.ExpireHash(key, TimeSpan.FromHours(1)));
        await CommitAsync(tx => tx.PersistHash(key));

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.Null(rows[0].ExpireAt);
    }

    // ── ExpireList / PersistList ──────────────────────────────────────────────

    [Fact]
    public async Task ExpireList_SetsExpireAt()
    {
        var key = "explist-" + Guid.NewGuid();
        await CommitAsync(tx => tx.InsertToList(key, "item"));
        await CommitAsync(tx => tx.ExpireList(key, TimeSpan.FromHours(1)));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.NotNull(rows[0].ExpireAt);
    }

    [Fact]
    public async Task PersistList_ClearsExpireAt()
    {
        var key = "perslist-" + Guid.NewGuid();
        await CommitAsync(tx => tx.InsertToList(key, "item"));
        await CommitAsync(tx => tx.ExpireList(key, TimeSpan.FromHours(1)));
        await CommitAsync(tx => tx.PersistList(key));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.Null(rows[0].ExpireAt);
    }

    // ── RemoveHash ────────────────────────────────────────────────────────────

    [Fact]
    public async Task RemoveHash_DeletesAllHashRows()
    {
        var key = "rmhash-" + Guid.NewGuid();
        await CommitAsync(tx => tx.SetRangeInHash(key,
            new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" }));
        await CommitAsync(tx => tx.RemoveHash(key));

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    // ── TrimList ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task TrimList_KeepsOnlySpecifiedRange()
    {
        var key = "trimlist-" + Guid.NewGuid();
        await CommitAsync(tx => {
            tx.InsertToList(key, "item0");
            tx.InsertToList(key, "item1");
            tx.InsertToList(key, "item2");
            tx.InsertToList(key, "item3");
        });
        await CommitAsync(tx => tx.TrimList(key, 1, 2));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task TrimList_RemovesAllRows_WhenStartBeyondCount()
    {
        var key = "trimlist-all-" + Guid.NewGuid();
        await CommitAsync(tx => tx.InsertToList(key, "only"));
        await CommitAsync(tx => tx.TrimList(key, 10, 20));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    // ── RemoveFromList ────────────────────────────────────────────────────────

    [Fact]
    public async Task RemoveFromList_DeletesMatchingRows()
    {
        var key = "rmlist-" + Guid.NewGuid();
        await CommitAsync(tx => {
            tx.InsertToList(key, "target");
            tx.InsertToList(key, "target");
            tx.InsertToList(key, "keep");
        });
        await CommitAsync(tx => tx.RemoveFromList(key, "target"));

        var rows = await _f.Storage.Lists.GetWhereAsync("Key", key);
        Assert.Single(rows);
        Assert.Equal("keep", rows[0].Value);
    }

    // ── AddToSet with score ───────────────────────────────────────────────────

    [Fact]
    public async Task AddToSet_WithScore_StoresScore()
    {
        var key = "scored-set-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "v", 3.14));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        var r = Assert.Single(rows);
        Assert.Equal(3.14, r.Score, 5);
    }

    [Fact]
    public async Task AddToSet_WithScore_UpdatesScore_WhenDuplicate()
    {
        var key = "scored-upd-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "v", 1.0));
        await CommitAsync(tx => tx.AddToSet(key, "v", 9.9));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        var r = Assert.Single(rows);
        Assert.Equal(9.9, r.Score, 5);
    }

    [Fact]
    public async Task AddToSet_DoesNotDuplicate_WhenKeyAndValueExist()
    {
        var key = "nodup-set-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddToSet(key, "v"));
        await CommitAsync(tx => tx.AddToSet(key, "v"));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Single(rows);
    }

    // ── AddRangeToSet ─────────────────────────────────────────────────────────

    [Fact]
    public async Task AddRangeToSet_AddsAllItems()
    {
        var key = "range-set-tx-" + Guid.NewGuid();
        await CommitAsync(tx => tx.AddRangeToSet(key, ["alpha", "beta", "gamma"]));

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);
        Assert.Equal(3, rows.Count);
    }

    // ── IncrementCounter / DecrementCounter with expiry ───────────────────────

    [Fact]
    public async Task IncrementCounter_WithExpiry_SetsExpireAt()
    {
        var key = "cnt-exp-inc-" + Guid.NewGuid();
        await CommitAsync(tx => tx.IncrementCounter(key, TimeSpan.FromHours(1)));

        var rows = await _f.Storage.Counters.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].ExpireAt);
    }

    [Fact]
    public async Task DecrementCounter_WithExpiry_SetsExpireAt()
    {
        var key = "cnt-exp-dec-" + Guid.NewGuid();
        await CommitAsync(tx => tx.DecrementCounter(key, TimeSpan.FromHours(1)));

        var rows = await _f.Storage.Counters.GetWhereAsync("Key", key);
        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].ExpireAt);
    }
}

[Collection("Sqlite")]
public class SqliteWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<SqliteFixture>
{
    public SqliteWriteOnlyTransactionFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<PostgresFixture>
{
    public PostgresWriteOnlyTransactionFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<SqlServerFixture>
{
    public SqlServerWriteOnlyTransactionFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<MySqlFixture>
{
    public MySqlWriteOnlyTransactionFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<OracleFixture>
{
    public OracleWriteOnlyTransactionFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<FirebirdFixture>
{
    public FirebirdWriteOnlyTransactionFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<CockroachDbFixture>
{
    public CockroachDbWriteOnlyTransactionFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<MariaDbFixture>
{
    public MariaDbWriteOnlyTransactionFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<DuckDbFixture>
{
    public DuckDbWriteOnlyTransactionFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<YugabyteDbFixture>
{
    public YugabyteDbWriteOnlyTransactionFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbWriteOnlyTransactionFacts : WriteOnlyTransactionFacts<TiDbFixture>
{
    public TiDbWriteOnlyTransactionFacts(TiDbFixture fixture) : base(fixture) { }
}
