using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Server;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;
using HangfireJob = Hangfire.Common.Job;

namespace pengdows.hangfire.tests;

/// <summary>
/// Unit tests for PengdowsCrudConnection covering null-argument guards,
/// invalid-jobId short-circuits, and SQL delegation to the storage gateways.
/// </summary>
public sealed class ConnectionTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage(
        SupportedDatabase db = SupportedDatabase.SqlServer)
    {
        var factory = new fakeDbFactory(db);
        var context  = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(context), factory);
    }

    private static PengdowsCrudConnection MakeConnection(
        SupportedDatabase db = SupportedDatabase.SqlServer)
        => new PengdowsCrudConnection(CreateStorage(db).Storage);

    private static bool NonQueryContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    private static bool ReaderContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedReaderTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    // ── constructor null guard ────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullStorage_Throws()
        => Assert.Throws<ArgumentNullException>(() => new PengdowsCrudConnection(null!));

    // ── FetchNextJob null guards ──────────────────────────────────────────────

    [Fact]
    public void FetchNextJob_NullQueues_ThrowsArgumentNull()
    {
        using var conn = MakeConnection();
        using var cts  = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<ArgumentNullException>(() => conn.FetchNextJob(null!, cts.Token));
    }

    [Fact]
    public void FetchNextJob_EmptyQueues_ThrowsArgumentNull()
    {
        using var conn = MakeConnection();
        using var cts  = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<ArgumentNullException>(() => conn.FetchNextJob(Array.Empty<string>(), cts.Token));
    }

    // ── CreateExpiredJob null guards ──────────────────────────────────────────

    [Fact]
    public void CreateExpiredJob_NullJob_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() =>
            conn.CreateExpiredJob(null!, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1)));
    }

    [Fact]
    public void CreateExpiredJob_NullParameters_Throws()
    {
        using var conn = MakeConnection();
        var job = HangfireJob.FromExpression(() => GC.Collect());
        Assert.Throws<ArgumentNullException>(() =>
            conn.CreateExpiredJob(job, null!, DateTime.UtcNow, TimeSpan.FromHours(1)));
    }

    // ── CreateExpiredJob – SQL delegation ────────────────────────────────────

    [Fact]
    public void CreateExpiredJob_IssuesInsertJobSql()
    {
        // Seed the auto-generated Job ID result (TableGateway reads scalar after INSERT)
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 1L } });
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);
        using var conn = new PengdowsCrudConnection(storage);

        var job    = HangfireJob.FromExpression(() => GC.Collect());
        var result = conn.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromHours(1));

        Assert.NotNull(result);
        // An INSERT into the Job table should have been fired
        Assert.True(NonQueryContains(factory, "INSERT") || ReaderContains(factory, "INSERT"));
    }

    // ── SetJobParameter ───────────────────────────────────────────────────────

    [Fact]
    public void SetJobParameter_NullJobId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.SetJobParameter(null!, "name", "val"));
    }

    [Fact]
    public void SetJobParameter_InvalidJobId_ReturnsWithoutQuery()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.SetJobParameter("not-a-number", "name", "val");  // should be a no-op
        Assert.Empty(factory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts));
    }

    [Fact]
    public void SetJobParameter_ValidJobId_IssuesUpsert()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.SetJobParameter("1", "RetryCount", "3");
        Assert.True(NonQueryContains(factory, "Name") || NonQueryContains(factory, "Value"));
    }

    // ── GetJobParameter ───────────────────────────────────────────────────────

    [Fact]
    public void GetJobParameter_NullJobId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetJobParameter(null!, "name"));
    }

    [Fact]
    public void GetJobParameter_InvalidJobId_ReturnsNull()
    {
        using var conn = MakeConnection();
        var result = conn.GetJobParameter("not-a-number", "name");
        Assert.Null(result);
    }

    [Fact]
    public void GetJobParameter_ValidJobId_NoRow_ReturnsNull()
    {
        using var conn = MakeConnection();
        var result = conn.GetJobParameter("42", "RetryCount");
        Assert.Null(result);
    }

    // ── GetJobData ────────────────────────────────────────────────────────────

    [Fact]
    public void GetJobData_NullJobId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetJobData(null!));
    }

    [Fact]
    public void GetJobData_InvalidJobId_ReturnsNull()
    {
        using var conn = MakeConnection();
        Assert.Null(conn.GetJobData("xyz"));
    }

    [Fact]
    public void GetJobData_ValidJobId_NoRow_ReturnsNull()
    {
        using var conn = MakeConnection();
        Assert.Null(conn.GetJobData("42"));
    }

    // ── GetStateData ──────────────────────────────────────────────────────────

    [Fact]
    public void GetStateData_NullJobId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetStateData(null!));
    }

    [Fact]
    public void GetStateData_InvalidJobId_ReturnsNull()
    {
        using var conn = MakeConnection();
        Assert.Null(conn.GetStateData("bad-id"));
    }

    [Fact]
    public void GetStateData_ValidJobId_NoRow_ReturnsNull()
    {
        using var conn = MakeConnection();
        Assert.Null(conn.GetStateData("1"));
    }

    // ── SetRangeInHash ────────────────────────────────────────────────────────

    [Fact]
    public void SetRangeInHash_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() =>
            conn.SetRangeInHash(null!, new Dictionary<string, string>()));
    }

    [Fact]
    public void SetRangeInHash_NullPairs_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.SetRangeInHash("k", null!));
    }

    [Fact]
    public void SetRangeInHash_ValidArgs_IssuesSql()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.SetRangeInHash("hk", new Dictionary<string, string> { ["field"] = "val" });
        Assert.True(NonQueryContains(factory, "Field") || NonQueryContains(factory, "Key"));
    }

    // ── GetAllEntriesFromHash ─────────────────────────────────────────────────

    [Fact]
    public void GetAllEntriesFromHash_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetAllEntriesFromHash(null!));
    }

    [Fact]
    public void GetAllEntriesFromHash_ValidKey_ReturnsEmptyWhenNoRows()
    {
        using var conn = MakeConnection();
        var result = conn.GetAllEntriesFromHash("hk");
        Assert.Empty(result);
    }

    // ── Server management ─────────────────────────────────────────────────────

    [Fact]
    public void AnnounceServer_NullServerId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() =>
            conn.AnnounceServer(null!, new ServerContext { Queues = new[] { "default" }, WorkerCount = 1 }));
    }

    [Fact]
    public void AnnounceServer_NullContext_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.AnnounceServer("srv", null!));
    }

    [Fact]
    public void AnnounceServer_ValidArgs_IssuesSql()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.AnnounceServer("server-1", new ServerContext { Queues = new[] { "default" }, WorkerCount = 2 });
        Assert.True(NonQueryContains(factory, "Data") || NonQueryContains(factory, "Heartbeat"));
    }

    [Fact]
    public void RemoveServer_NullServerId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.RemoveServer(null!));
    }

    [Fact]
    public void RemoveServer_ValidServerId_IssuesSql()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.RemoveServer("srv");
        Assert.True(NonQueryContains(factory, "DELETE") || NonQueryContains(factory, "Id"));
    }

    [Fact]
    public void Heartbeat_NullServerId_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.Heartbeat(null!));
    }

    [Fact]
    public void Heartbeat_ValidServerId_IssuesSql()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.Heartbeat("srv");
        Assert.True(NonQueryContains(factory, "Heartbeat") || NonQueryContains(factory, "UPDATE"));
    }

    [Fact]
    public void RemoveTimedOutServers_IssuesSql()
    {
        var (storage, factory) = CreateStorage();
        using var conn = new PengdowsCrudConnection(storage);
        conn.RemoveTimedOutServers(TimeSpan.FromMinutes(5));
        // Either reader (select to find timed-out) or non-query (delete) should have been issued
        var anySql = factory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
            .Concat(factory.CreatedConnections.SelectMany(c => c.ExecutedReaderTexts))
            .Any();
        Assert.True(anySql);
    }

    // ── Set queries ───────────────────────────────────────────────────────────

    [Fact]
    public void GetAllItemsFromSet_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetAllItemsFromSet(null!));
    }

    [Fact]
    public void GetAllItemsFromSet_ValidKey_ReturnsEmptyWhenNoRows()
    {
        using var conn = MakeConnection();
        var result = conn.GetAllItemsFromSet("sk");
        Assert.Empty(result);
    }

    [Fact]
    public void GetFirstByLowestScoreFromSet_Single_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetFirstByLowestScoreFromSet(null!, 0, 1));
    }

    [Fact]
    public void GetFirstByLowestScoreFromSet_Multi_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetFirstByLowestScoreFromSet(null!, 0, 1, 3));
    }

    [Fact]
    public void GetSetCount_NullKey_Throws()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        using var conn = new PengdowsCrudConnection(new PengdowsCrudJobStorage(ctx));
        Assert.Throws<ArgumentNullException>(() => conn.GetSetCount((string)null!));
    }

    [Fact]
    public void GetSetCount_Multi_NullKeys_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetSetCount((IEnumerable<string>)null!, 10));
    }

    [Fact]
    public void GetSetCount_Multi_ReturnsZeroWhenEmpty()
    {
        using var conn = MakeConnection();
        var result = conn.GetSetCount(Array.Empty<string>(), 100);
        Assert.Equal(0L, result);
    }

    [Fact]
    public void GetSetContains_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetSetContains(null!, "v"));
    }

    [Fact]
    public void GetSetContains_NullValue_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetSetContains("k", null!));
    }

    [Fact]
    public void GetRangeFromSet_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetRangeFromSet(null!, 0, 9));
    }

    [Fact]
    public void GetRangeFromSet_ValidKey_ReturnsEmptyWhenNoRows()
    {
        using var conn = MakeConnection();
        Assert.Empty(conn.GetRangeFromSet("sk", 0, 9));
    }

    [Fact]
    public void GetSetTtl_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetSetTtl(null!));
    }

    [Fact]
    public void GetSetTtl_NoExpiry_ReturnsNegativeOne()
    {
        using var conn = MakeConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetSetTtl("sk"));
    }

    // ── Hash queries ──────────────────────────────────────────────────────────

    [Fact]
    public void GetValueFromHash_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetValueFromHash(null!, "f"));
    }

    [Fact]
    public void GetValueFromHash_NullName_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetValueFromHash("k", null!));
    }

    [Fact]
    public void GetValueFromHash_NoRow_ReturnsNull()
    {
        using var conn = MakeConnection();
        Assert.Null(conn.GetValueFromHash("k", "f"));
    }

    [Fact]
    public void GetHashCount_NullKey_Throws()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        using var conn = new PengdowsCrudConnection(new PengdowsCrudJobStorage(ctx));
        Assert.Throws<ArgumentNullException>(() => conn.GetHashCount(null!));
    }

    [Fact]
    public void GetHashTtl_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetHashTtl(null!));
    }

    [Fact]
    public void GetHashTtl_NoExpiry_ReturnsNegativeOne()
    {
        using var conn = MakeConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetHashTtl("k"));
    }

    // ── List queries ──────────────────────────────────────────────────────────

    [Fact]
    public void GetListCount_NullKey_Throws()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        using var conn = new PengdowsCrudConnection(new PengdowsCrudJobStorage(ctx));
        Assert.Throws<ArgumentNullException>(() => conn.GetListCount(null!));
    }

    [Fact]
    public void GetListTtl_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetListTtl(null!));
    }

    [Fact]
    public void GetListTtl_NoExpiry_ReturnsNegativeOne()
    {
        using var conn = MakeConnection();
        Assert.Equal(TimeSpan.FromSeconds(-1), conn.GetListTtl("lk"));
    }

    [Fact]
    public void GetRangeFromList_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetRangeFromList(null!, 0, 9));
    }

    [Fact]
    public void GetRangeFromList_ValidKey_ReturnsEmptyWhenNoRows()
    {
        using var conn = MakeConnection();
        Assert.Empty(conn.GetRangeFromList("lk", 0, 9));
    }

    [Fact]
    public void GetAllItemsFromList_NullKey_Throws()
    {
        using var conn = MakeConnection();
        Assert.Throws<ArgumentNullException>(() => conn.GetAllItemsFromList(null!));
    }

    [Fact]
    public void GetAllItemsFromList_ValidKey_ReturnsEmptyWhenNoRows()
    {
        using var conn = MakeConnection();
        Assert.Empty(conn.GetAllItemsFromList("lk"));
    }

    // ── GetUtcDateTime ────────────────────────────────────────────────────────

    [Fact]
    public void GetUtcDateTime_ReturnsCurrentUtcWithinTolerance()
    {
        using var conn = MakeConnection();
        var before = DateTime.UtcNow;
        var result = conn.GetUtcDateTime();
        var after  = DateTime.UtcNow;

        Assert.InRange(result, before.AddSeconds(-1), after.AddSeconds(1));
        Assert.Equal(DateTimeKind.Utc, result.Kind);
    }

    // ── CreateWriteTransaction / AcquireDistributedLock ───────────────────────

    [Fact]
    public void CreateWriteTransaction_ReturnsNonNull()
    {
        using var conn = MakeConnection();
        using var tx   = conn.CreateWriteTransaction();
        Assert.NotNull(tx);
    }
}
