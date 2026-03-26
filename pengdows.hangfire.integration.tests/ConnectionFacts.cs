using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using pengdows.hangfire.models;
using Xunit;
using HangfireJob = Hangfire.Common.Job;

namespace pengdows.hangfire.integration.tests;

public abstract class ConnectionFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected ConnectionFacts(TFixture fixture) => _f = fixture;

    private PengdowsCrudConnection OpenConnection() =>
        (PengdowsCrudConnection)_f.Storage.GetConnection();

    // ── CreateExpiredJob ──────────────────────────────────────────────────────

    [Fact]
    public void CreateExpiredJob_ReturnsJobId()
    {
        using var conn = OpenConnection();
        var job = HangfireJob.FromExpression(() => Console.WriteLine("test"));
        var id = conn.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));
        Assert.NotNull(id);
        Assert.True(long.TryParse(id, out _));
    }

    [Fact]
    public void CreateExpiredJob_StoresParameters()
    {
        using var conn = OpenConnection();
        var job = HangfireJob.FromExpression(() => Console.WriteLine("test"));
        var id = conn.CreateExpiredJob(job,
            new Dictionary<string, string> { ["RetryCount"] = "3", ["CurrentCulture"] = "en-US" },
            DateTime.UtcNow, TimeSpan.FromHours(1));

        Assert.Equal("3",    conn.GetJobParameter(id, "RetryCount"));
        Assert.Equal("en-US", conn.GetJobParameter(id, "CurrentCulture"));
    }

    [Fact]
    public async Task CreateExpiredJob_SetsExpireAt()
    {
        using var conn = OpenConnection();
        var job = HangfireJob.FromExpression(() => Console.WriteLine("test"));
        var id = conn.CreateExpiredJob(job, new Dictionary<string, string>(),
            DateTime.UtcNow, TimeSpan.FromHours(2));

        var jobData = await _f.Storage.Jobs.RetrieveOneAsync(long.Parse(id));
        Assert.NotNull(jobData);
        Assert.NotNull(jobData.ExpireAt);
        Assert.True(jobData.ExpireAt > DateTime.UtcNow);
    }

    // ── GetJobData ────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetJobData_ReturnsNull_WhenJobDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetJobData("99999");
        Assert.Null(result);
    }

    [Fact]
    public async Task GetJobData_ReturnsJobData_WhenJobExists()
    {
        // Build valid InvocationData using the same serialization path as production code.
        var job = HangfireJob.FromExpression(() => Console.WriteLine("test"));
        var invData = Hangfire.Common.SerializationHelper.Serialize(InvocationData.SerializeJob(job), Hangfire.Common.SerializationOption.User);
        var jobId = await _f.InsertJobAsync(invocationData: invData, stateName: "Enqueued");

        using var conn = OpenConnection();
        var data = conn.GetJobData(jobId.ToString());

        Assert.NotNull(data);
        Assert.Equal("Enqueued", data!.State);
    }

    // ── GetStateData ──────────────────────────────────────────────────────────

    [Fact]
    public async Task GetStateData_ReturnsNull_WhenJobHasNoState()
    {
        var jobId = await _f.InsertJobAsync();

        using var conn = OpenConnection();
        var result = conn.GetStateData(jobId.ToString());
        Assert.Null(result);
    }

    // ── GetJobParameter / SetJobParameter ─────────────────────────────────────

    [Fact]
    public async Task GetJobParameter_ReturnsNull_WhenParameterDoesNotExist()
    {
        var jobId = await _f.InsertJobAsync();

        using var conn = OpenConnection();
        var result = conn.GetJobParameter(jobId.ToString(), "NoSuch");
        Assert.Null(result);
    }

    [Fact]
    public async Task SetAndGet_JobParameter_RoundTrips()
    {
        var jobId = await _f.InsertJobAsync();

        using var conn = OpenConnection();
        conn.SetJobParameter(jobId.ToString(), "TestParam", "hello");
        var result = conn.GetJobParameter(jobId.ToString(), "TestParam");
        Assert.Equal("hello", result);
    }

    // ── SetJobParameter upsert ────────────────────────────────────────────────

    [Fact]
    public async Task SetJobParameter_UpdatesExistingParameter()
    {
        var jobId = await _f.InsertJobAsync();

        using var conn = OpenConnection();
        conn.SetJobParameter(jobId.ToString(), "key", "original");
        conn.SetJobParameter(jobId.ToString(), "key", "updated");

        Assert.Equal("updated", conn.GetJobParameter(jobId.ToString(), "key"));
    }

    [Fact]
    public async Task SetJobParameter_CanStoreNullValue()
    {
        var jobId = await _f.InsertJobAsync();

        using var conn = OpenConnection();
        conn.SetJobParameter(jobId.ToString(), "nullable", null!);
        var result = conn.GetJobParameter(jobId.ToString(), "nullable");
        Assert.Null(result);
    }

    // ── GetUtcDateTime ────────────────────────────────────────────────────────

    [Fact]
    public void GetUtcDateTime_ReturnsCurrentUtc()
    {
        using var conn = OpenConnection();
        var before = DateTime.UtcNow.AddSeconds(-1);
        var dt = conn.GetUtcDateTime();
        var after = DateTime.UtcNow.AddSeconds(1);

        Assert.Equal(DateTimeKind.Utc, dt.Kind);
        Assert.True(dt >= before && dt <= after);
    }

    // ── GetAllItemsFromSet ────────────────────────────────────────────────────

    [Fact]
    public async Task GetAllItemsFromSet_ReturnsEmptySet_WhenSetDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetAllItemsFromSet("nosuchset");
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetAllItemsFromSet_ReturnsItems_AfterAddToSet()
    {
        await _f.InsertSetAsync("myset-conn", "v1");
        await _f.InsertSetAsync("myset-conn", "v2");

        using var conn = OpenConnection();
        var result = conn.GetAllItemsFromSet("myset-conn");
        Assert.Equal(2, result.Count);
    }

    // ── GetAllEntriesFromHash ─────────────────────────────────────────────────

    [Fact]
    public async Task GetAllEntriesFromHash_ReturnsEmpty_WhenHashDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetAllEntriesFromHash("nosuchhash");
        // Implementation returns an empty dict (not null) when the key does not exist.
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetAllEntriesFromHash_ReturnsFields_AfterSetRangeInHash()
    {
        await _f.InsertHashAsync("myhash-conn", "f1", "v1");
        await _f.InsertHashAsync("myhash-conn", "f2", "v2");

        using var conn = OpenConnection();
        var result = conn.GetAllEntriesFromHash("myhash-conn");
        Assert.NotNull(result);
        Assert.Equal(2, result!.Count);
        Assert.Equal("v1", result["f1"]);
    }

    // ── GetSetCount / GetHashCount / GetListCount ─────────────────────────────

    [Fact]
    public async Task GetSetCount_ReturnsZero_ForMissingKey()
    {
        using var conn = OpenConnection();
        Assert.Equal(0, conn.GetSetCount("nosuchset-cnt"));
    }

    [Fact]
    public async Task GetHashCount_ReturnsZero_ForMissingKey()
    {
        using var conn = OpenConnection();
        Assert.Equal(0, conn.GetHashCount("nosuchhash-cnt"));
    }

    [Fact]
    public async Task GetListCount_ReturnsZero_ForMissingKey()
    {
        using var conn = OpenConnection();
        Assert.Equal(0, conn.GetListCount("nosuchlist-cnt"));
    }

    // ── GetSetCount / GetHashCount / GetListCount (with data) ─────────────────

    [Fact]
    public async Task GetSetCount_ReturnsCorrectCount()
    {
        var key = "cnt-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "a");
        await _f.InsertSetAsync(key, "b");

        using var conn = OpenConnection();
        Assert.Equal(2, conn.GetSetCount(key));
    }

    [Fact]
    public async Task GetHashCount_ReturnsCorrectCount()
    {
        var key = "cnt-hash-" + Guid.NewGuid();
        await _f.InsertHashAsync(key, "f1");
        await _f.InsertHashAsync(key, "f2");
        await _f.InsertHashAsync(key, "f3");

        using var conn = OpenConnection();
        Assert.Equal(3, conn.GetHashCount(key));
    }

    [Fact]
    public async Task GetListCount_ReturnsCorrectCount()
    {
        var key = "cnt-list-" + Guid.NewGuid();
        await _f.InsertListAsync(key, "x");
        await _f.InsertListAsync(key, "y");

        using var conn = OpenConnection();
        Assert.Equal(2, conn.GetListCount(key));
    }

    // ── GetSetTtl / GetHashTtl / GetListTtl ───────────────────────────────────

    [Fact]
    public async Task GetSetTtl_ReturnsNegativeOne_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetSetTtl("nosuchset-ttl"));
    }

    [Fact]
    public async Task GetSetTtl_ReturnsExpiration()
    {
        var key = "ttl-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "v", expireAt: DateTime.UtcNow.AddHours(1));

        using var conn = OpenConnection();
        var ttl = conn.GetSetTtl(key);
        Assert.True(ttl > TimeSpan.Zero && ttl <= TimeSpan.FromHours(1));
    }

    [Fact]
    public async Task GetHashTtl_ReturnsNegativeOne_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetHashTtl("nosuchhash-ttl"));
    }

    [Fact]
    public async Task GetHashTtl_ReturnsExpiration()
    {
        var key = "ttl-hash-" + Guid.NewGuid();
        await _f.InsertHashAsync(key, "f1", expireAt: DateTime.UtcNow.AddHours(2));

        using var conn = OpenConnection();
        var ttl = conn.GetHashTtl(key);
        Assert.True(ttl > TimeSpan.Zero && ttl <= TimeSpan.FromHours(2));
    }

    [Fact]
    public async Task GetListTtl_ReturnsNegativeOne_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetListTtl("nosuchlist-ttl"));
    }

    [Fact]
    public async Task GetListTtl_ReturnsExpiration()
    {
        var key = "ttl-list-" + Guid.NewGuid();
        await _f.InsertListAsync(key, expireAt: DateTime.UtcNow.AddHours(3));

        using var conn = OpenConnection();
        var ttl = conn.GetListTtl(key);
        Assert.True(ttl > TimeSpan.Zero && ttl <= TimeSpan.FromHours(3));
    }

    // ── GetRangeFromList ──────────────────────────────────────────────────────

    [Fact]
    public async Task GetRangeFromList_ReturnsEmpty_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetRangeFromList("nosuchlist-range", 0, 10);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetRangeFromList_ReturnsItemsWithinBounds()
    {
        var key = "range-list-" + Guid.NewGuid();
        await _f.InsertListAsync(key, "item0");
        await _f.InsertListAsync(key, "item1");
        await _f.InsertListAsync(key, "item2");

        using var conn = OpenConnection();
        var result = conn.GetRangeFromList(key, 0, 1);
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task GetAllItemsFromList_ReturnsAll()
    {
        var key = "all-list-" + Guid.NewGuid();
        await _f.InsertListAsync(key, "a");
        await _f.InsertListAsync(key, "b");
        await _f.InsertListAsync(key, "c");

        using var conn = OpenConnection();
        var result = conn.GetAllItemsFromList(key);
        Assert.Equal(3, result.Count);
    }

    // ── GetRangeFromSet ───────────────────────────────────────────────────────

    [Fact]
    public async Task GetRangeFromSet_ReturnsEmpty_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetRangeFromSet("nosuchset-range", 0, 10);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetRangeFromSet_ReturnsPagedElements()
    {
        var key = "range-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "a", 1.0);
        await _f.InsertSetAsync(key, "b", 2.0);
        await _f.InsertSetAsync(key, "c", 3.0);

        using var conn = OpenConnection();
        var result = conn.GetRangeFromSet(key, 0, 1);
        Assert.Equal(2, result.Count);
    }

    // ── GetFirstByLowestScoreFromSet ──────────────────────────────────────────

    [Fact]
    public async Task GetFirstByLowestScoreFromSet_ReturnsNull_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetFirstByLowestScoreFromSet("nosuchset-score", 0, 100);
        Assert.Null(result);
    }

    [Fact]
    public async Task GetFirstByLowestScoreFromSet_ReturnsLowestScoreValue()
    {
        var key = "score-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "low",  1.0);
        await _f.InsertSetAsync(key, "high", 9.0);

        using var conn = OpenConnection();
        var result = conn.GetFirstByLowestScoreFromSet(key, 0, 10);
        Assert.Equal("low", result);
    }

    [Fact]
    public async Task GetFirstByLowestScoreFromSet_List_ReturnsEmpty_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        var result = conn.GetFirstByLowestScoreFromSet("nosuchset-scorelist", 0, 100, 5);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetFirstByLowestScoreFromSet_List_ReturnsCorrectItems()
    {
        var key = "score-list-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "a", 1.0);
        await _f.InsertSetAsync(key, "b", 2.0);
        await _f.InsertSetAsync(key, "c", 3.0);

        using var conn = OpenConnection();
        var result = conn.GetFirstByLowestScoreFromSet(key, 0, 10, 2);
        Assert.Equal(2, result.Count);
        Assert.Equal("a", result[0]);
        Assert.Equal("b", result[1]);
    }

    // ── GetSetContains ────────────────────────────────────────────────────────

    [Fact]
    public async Task GetSetContains_ReturnsFalse_WhenKeyDoesNotExist()
    {
        using var conn = OpenConnection();
        Assert.False(conn.GetSetContains("nosuchset-contains", "v"));
    }

    [Fact]
    public async Task GetSetContains_ReturnsTrue_WhenValueExists()
    {
        var key = "contains-set-" + Guid.NewGuid();
        await _f.InsertSetAsync(key, "present");

        using var conn = OpenConnection();
        Assert.True(conn.GetSetContains(key, "present"));
        Assert.False(conn.GetSetContains(key, "absent"));
    }

    // ── GetValueFromHash ──────────────────────────────────────────────────────

    [Fact]
    public async Task GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
    {
        using var conn = OpenConnection();
        Assert.Null(conn.GetValueFromHash("nosuchhash-val", "field"));
    }

    [Fact]
    public async Task GetValueFromHash_ReturnsValue()
    {
        var key = "val-hash-" + Guid.NewGuid();
        await _f.InsertHashAsync(key, "myfield", "myvalue");

        using var conn = OpenConnection();
        Assert.Equal("myvalue", conn.GetValueFromHash(key, "myfield"));
    }

    // ── GetStateData ─────────────────────────────────────────────────────────

    [Fact]
    public async Task GetStateData_ReturnsData_WhenStateExists()
    {
        var jobId = await _f.InsertJobAsync(stateName: "Failed");
        // Insert a state row directly
        await _f.Storage.JobStates.CreateAsync(new State { 
            JobID = jobId, 
            Name = "Failed", 
            CreatedAt = DateTime.UtcNow, 
            Data = "{\"ExceptionMessage\":\"boom\"}" 
        });

        using var conn = OpenConnection();
        var state = conn.GetStateData(jobId.ToString());

        Assert.NotNull(state);
        Assert.Equal("Failed", state!.Name);
    }

    // ── AnnounceServer / RemoveServer / Heartbeat ─────────────────────────────

    [Fact]
    public async Task AnnounceServer_DoesNotThrow()
    {
        using var conn = OpenConnection();
        conn.AnnounceServer("server-1", new Hangfire.Server.ServerContext { WorkerCount = 5, Queues = ["default"] });
    }

    [Fact]
    public async Task AnnounceServer_StoresServerRecord()
    {
        var id = "server-stored-" + Guid.NewGuid();
        using var conn = OpenConnection();
        conn.AnnounceServer(id, new Hangfire.Server.ServerContext { WorkerCount = 3, Queues = ["default"] });

        var server = await _f.Storage.Servers.RetrieveOneAsync(id);
        Assert.NotNull(server);
    }

    [Fact]
    public async Task Heartbeat_DoesNotThrow_AfterAnnounce()
    {
        using var conn = OpenConnection();
        conn.AnnounceServer("server-hb", new Hangfire.Server.ServerContext { WorkerCount = 1, Queues = ["default"] });
        conn.Heartbeat("server-hb");
    }

    [Fact]
    public async Task RemoveServer_DoesNotThrow()
    {
        using var conn = OpenConnection();
        conn.AnnounceServer("server-rm", new Hangfire.Server.ServerContext { WorkerCount = 1, Queues = ["default"] });
        conn.RemoveServer("server-rm");
    }

    [Fact]
    public async Task RemoveServer_DeletesRecord()
    {
        var id = "server-deleted-" + Guid.NewGuid();
        using var conn = OpenConnection();
        conn.AnnounceServer(id, new Hangfire.Server.ServerContext { WorkerCount = 1, Queues = ["default"] });
        conn.RemoveServer(id);

        var server = await _f.Storage.Servers.RetrieveOneAsync(id);
        Assert.Null(server);
    }

    [Fact]
    public async Task Heartbeat_UpdatesLastHeartbeat()
    {
        var id = "server-hb-upd-" + Guid.NewGuid();
        using var conn = OpenConnection();
        conn.AnnounceServer(id, new Hangfire.Server.ServerContext { WorkerCount = 1, Queues = ["default"] });

        var server1 = await _f.Storage.Servers.RetrieveOneAsync(id);
        Assert.NotNull(server1);
        var before = server1.LastHeartbeat;

        await Task.Delay(100);
        conn.Heartbeat(id);

        var server2 = await _f.Storage.Servers.RetrieveOneAsync(id);
        Assert.NotNull(server2);
        Assert.True(server2.LastHeartbeat > before);
    }

    [Fact]
    public async Task RemoveTimedOutServers_RemovesStaleServers()
    {
        var staleId = "server-stale-" + Guid.NewGuid();
        var activeId = "server-active-" + Guid.NewGuid();

        // Insert stale server with old heartbeat directly
        await _f.Storage.Servers.CreateAsync(new pengdows.hangfire.models.Server { 
            ID = staleId, 
            Data = "{}", 
            LastHeartbeat = DateTime.UtcNow.AddHours(-2) 
        });

        using var conn = OpenConnection();
        conn.AnnounceServer(activeId, new Hangfire.Server.ServerContext { WorkerCount = 1, Queues = ["default"] });

        var removed = conn.RemoveTimedOutServers(TimeSpan.FromHours(1));

        Assert.True(removed >= 1);
        
        Assert.Null(await _f.Storage.Servers.RetrieveOneAsync(staleId));
        Assert.NotNull(await _f.Storage.Servers.RetrieveOneAsync(activeId));
    }

    // ── GetSetCount with keys+limit ───────────────────────────────────────────

    [Fact]
    public async Task GetSetCount_WithMultipleKeys_SumsCappedAtLimit()
    {
        var key1 = "setcnt-lim-a-" + Guid.NewGuid();
        var key2 = "setcnt-lim-b-" + Guid.NewGuid();
        await _f.InsertSetAsync(key1, "v1");
        await _f.InsertSetAsync(key1, "v2");
        await _f.InsertSetAsync(key2, "v1");

        using var conn = OpenConnection();
        var result = conn.GetSetCount([key1, key2], 2);
        Assert.Equal(2, result); // capped at limit=2 even though total is 3
    }

    // ── AcquireDistributedLock via connection ─────────────────────────────────

    [Fact]
    public void AcquireDistributedLock_ReturnsDisposable()
    {
        using var conn = OpenConnection();
        using var lk = conn.AcquireDistributedLock("conn-lock-" + Guid.NewGuid(), TimeSpan.FromSeconds(5));
        Assert.NotNull(lk);
    }
}

[Collection("Sqlite")]
public class SqliteConnectionFacts : ConnectionFacts<SqliteFixture>
{
    public SqliteConnectionFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresConnectionFacts : ConnectionFacts<PostgresFixture>
{
    public PostgresConnectionFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerConnectionFacts : ConnectionFacts<SqlServerFixture>
{
    public SqlServerConnectionFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlConnectionFacts : ConnectionFacts<MySqlFixture>
{
    public MySqlConnectionFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleConnectionFacts : ConnectionFacts<OracleFixture>
{
    public OracleConnectionFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdConnectionFacts : ConnectionFacts<FirebirdFixture>
{
    public FirebirdConnectionFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbConnectionFacts : ConnectionFacts<CockroachDbFixture>
{
    public CockroachDbConnectionFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbConnectionFacts : ConnectionFacts<MariaDbFixture>
{
    public MariaDbConnectionFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbConnectionFacts : ConnectionFacts<DuckDbFixture>
{
    public DuckDbConnectionFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbConnectionFacts : ConnectionFacts<YugabyteDbFixture>
{
    public YugabyteDbConnectionFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbConnectionFacts : ConnectionFacts<TiDbFixture>
{
    public TiDbConnectionFacts(TiDbFixture fixture) : base(fixture) { }
}
