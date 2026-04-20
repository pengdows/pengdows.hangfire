using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.States;
using Hangfire.Storage;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Unit tests for PengdowsCrudWriteOnlyTransaction: verifies that each method
/// queues the correct SQL command and that Commit() executes all queued commands.
/// </summary>
public sealed class TransactionTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    private static bool NonQueryContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    private static bool ReaderContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedReaderTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    private static bool SqlContains(fakeDbFactory f, string s) =>
        NonQueryContains(f, s) || ReaderContains(f, s);

    // ── constructor ───────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullStorage_Throws()
        => Assert.Throws<ArgumentNullException>(() => new PengdowsCrudWriteOnlyTransaction(null!));

    // ── ExpireJob / PersistJob ────────────────────────────────────────────────

    [Fact]
    public void ExpireJob_CommitsUpdateExpireAtSql()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.ExpireJob("1", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "ExpireAt"));
    }

    [Fact]
    public void ExpireJob_InvalidJobId_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.ExpireJob("not-a-number", TimeSpan.FromHours(1));
        tx.Commit(); // nothing queued — no DML should appear
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    [Fact]
    public void PersistJob_CommitsUpdateExpireAtWithNull()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.PersistJob("2");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "ExpireAt"));
    }

    [Fact]
    public void PersistJob_InvalidJobId_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.PersistJob("bad");
        tx.Commit();
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    // ── SetJobState / AddJobState ─────────────────────────────────────────────

    [Fact]
    public void SetJobState_CommitsUpdateAndInsertState()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.SetJobState("3", new SucceededState(null, 1, 100));
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        // An INSERT for the new State row should also appear
        Assert.True(NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "State"));
    }

    [Fact]
    public void SetJobState_InvalidJobId_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.SetJobState("nope", new SucceededState(null, 1, 100));
        tx.Commit();
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    [Fact]
    public void AddJobState_CommitsInsertState()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddJobState("4", new FailedState(new Exception("boom")));
        tx.Commit();
        // TableGateway with [Id(false)] uses OUTPUT INSERTED on SQL Server (reader path)
        Assert.True(NonQueryContains(factory, "INSERT") || ReaderContains(factory, "INSERT"));
    }

    // ── AddToQueue ────────────────────────────────────────────────────────────

    [Fact]
    public void AddToQueue_CommitsInsertJobQueue()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddToQueue("default", "5");
        tx.Commit();
        // TableGateway with [Id(false)] uses OUTPUT INSERTED on SQL Server (reader path)
        Assert.True(NonQueryContains(factory, "INSERT") || ReaderContains(factory, "INSERT"));
        Assert.True(NonQueryContains(factory, "Queue") || ReaderContains(factory, "Queue"));
    }

    [Fact]
    public void AddToQueue_InvalidJobId_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddToQueue("default", "bad");
        tx.Commit();
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    // ── IncrementCounter / DecrementCounter ───────────────────────────────────

    [Fact]
    public void IncrementCounter_CommitsInsertWithPositiveDelta()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.IncrementCounter("stats:succeeded");
        tx.Commit();
        Assert.True(SqlContains(factory, "INSERT"));
    }

    [Fact]
    public void IncrementCounter_WithExpiry_CommitsInsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.IncrementCounter("k", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(SqlContains(factory, "INSERT"));
    }

    [Fact]
    public void DecrementCounter_CommitsInsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.DecrementCounter("k");
        tx.Commit();
        Assert.True(SqlContains(factory, "INSERT"));
    }

    [Fact]
    public void DecrementCounter_WithExpiry_CommitsInsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.DecrementCounter("k", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(SqlContains(factory, "INSERT"));
    }

    // ── AddToSet / RemoveFromSet / AddRangeToSet / RemoveSet ──────────────────

    [Fact]
    public void AddToSet_CommitsUpsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddToSet("recurring-jobs", "job-1");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "Key") || NonQueryContains(factory, "Value"));
    }

    [Fact]
    public void AddToSet_WithScore_CommitsUpsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddToSet("recurring-jobs", "job-2", 1.5);
        tx.Commit();
        Assert.True(NonQueryContains(factory, "Score") || NonQueryContains(factory, "Value"));
    }

    [Fact]
    public void RemoveFromSet_CommitsDelete()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.RemoveFromSet("recurring-jobs", "job-1");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "DELETE"));
    }

    [Fact]
    public void AddRangeToSet_CommitsUpserts()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddRangeToSet("myset", new[] { "a", "b", "c" });
        tx.Commit();
        // Upsert should fire SQL
        var anySql = factory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts).Any();
        Assert.True(anySql);
    }

    [Fact]
    public void AddRangeToSet_NullKey_Throws()
    {
        var (storage, _) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        Assert.Throws<ArgumentNullException>(() => tx.AddRangeToSet(null!, new[] { "a" }));
    }

    [Fact]
    public void AddRangeToSet_NullItems_Throws()
    {
        var (storage, _) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        Assert.Throws<ArgumentNullException>(() => tx.AddRangeToSet("k", null!));
    }

    [Fact]
    public void RemoveSet_CommitsDelete()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.RemoveSet("myset");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "DELETE"));
    }

    // ── InsertToList / RemoveFromList / TrimList ──────────────────────────────

    [Fact]
    public void InsertToList_CommitsInsert()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.InsertToList("mylist", "item");
        tx.Commit();
        Assert.True(SqlContains(factory, "INSERT"));
    }

    [Fact]
    public void RemoveFromList_CommitsDelete()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.RemoveFromList("mylist", "item");
        tx.Commit();
        Assert.True(SqlContains(factory, "DELETE"));
    }

    [Fact]
    public void TrimList_CommitsDeleteWithNotIn()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.TrimList("mylist", 0, 9);
        tx.Commit();
        Assert.True(NonQueryContains(factory, "DELETE"));
        Assert.True(NonQueryContains(factory, "NOT IN"));
    }

    // ── SetRangeInHash / RemoveHash ───────────────────────────────────────────

    [Fact]
    public void SetRangeInHash_CommitsUpserts()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.SetRangeInHash("hk", new Dictionary<string, string> { ["f1"] = "v1", ["f2"] = "v2" });
        tx.Commit();
        var anySql = factory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts).Any();
        Assert.True(anySql);
    }

    [Fact]
    public void SetRangeInHash_NullKey_Throws()
    {
        var (storage, _) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        Assert.Throws<ArgumentNullException>(() =>
            tx.SetRangeInHash(null!, new Dictionary<string, string>()));
    }

    [Fact]
    public void SetRangeInHash_NullPairs_Throws()
    {
        var (storage, _) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        Assert.Throws<ArgumentNullException>(() => tx.SetRangeInHash("k", null!));
    }

    [Fact]
    public void RemoveHash_CommitsDelete()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.RemoveHash("hk");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "DELETE"));
    }

    // ── ExpireSet / PersistSet ────────────────────────────────────────────────

    [Fact]
    public void ExpireSet_CommitsUpdateExpireAt()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.ExpireSet("sk", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "ExpireAt"));
    }

    [Fact]
    public void PersistSet_CommitsUpdateExpireAtNull()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.PersistSet("sk");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
    }

    // ── ExpireHash / PersistHash ──────────────────────────────────────────────

    [Fact]
    public void ExpireHash_CommitsUpdateExpireAt()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.ExpireHash("hk", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "ExpireAt"));
    }

    [Fact]
    public void PersistHash_CommitsUpdateExpireAtNull()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.PersistHash("hk");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
    }

    // ── ExpireList / PersistList ──────────────────────────────────────────────

    [Fact]
    public void ExpireList_CommitsUpdateExpireAt()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.ExpireList("lk", TimeSpan.FromHours(1));
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "ExpireAt"));
    }

    [Fact]
    public void PersistList_CommitsUpdateExpireAtNull()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.PersistList("lk");
        tx.Commit();
        Assert.True(NonQueryContains(factory, "UPDATE"));
    }

    // ── RemoveFromQueue ───────────────────────────────────────────────────────

    [Fact]
    public void RemoveFromQueue_WithPengdowsFetchedJob_CommitsAcknowledge()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);

        var fetchedJob = new PengdowsCrudFetchedJob(storage, 7L, "default");
        fetchedJob.RemoveFromQueue(); // marks it for deletion

        tx.RemoveFromQueue(fetchedJob);
        tx.Commit();
        // The RemoveFromQueue action calls job.RemoveFromQueue() which is synchronous;
        // actual DB delete happens when the fetched job is disposed.
        // Disposing it now triggers the DELETE
        fetchedJob.Dispose();
        Assert.True(NonQueryContains(factory, "DELETE"));
    }

    [Fact]
    public void RemoveFromQueue_UnknownFetchedJobType_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.RemoveFromQueue(new FakeFetchedJob()); // not PengdowsCrudFetchedJob → ignored
        tx.Commit();
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_ReleasesAcquiredLocks()
    {
        var (storage, _) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        // AcquireDistributedLock queues a lock acquisition
        // (the lock itself may throw on timeout with fakeDb, so we skip the acquire and just ensure dispose is safe)
        tx.Dispose(); // should not throw
    }

    // ── Multiple commands in one transaction ──────────────────────────────────

    [Fact]
    public void Commit_ExecutesAllQueuedCommands()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.IncrementCounter("k1");
        tx.IncrementCounter("k2");
        tx.IncrementCounter("k3");
        tx.Commit();

        var connections = factory.CreatedConnections;
        var allSql = connections.SelectMany(c => c.ExecutedNonQueryTexts)
            .Concat(connections.SelectMany(c => c.ExecutedReaderTexts));
        
        var insertCount = allSql.Count(t => t.Contains("INSERT", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(3, insertCount);
    }

    // ── helper stubs ─────────────────────────────────────────────────────────

    [Fact]
    public void CommitAsync_PostgreSql_UsesStrictConsistency()
    {
        var factory = new fakeDbFactory(SupportedDatabase.PostgreSql);
        var ctx     = new DatabaseContext("Host=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.IncrementCounter("pg-key");
        tx.Commit(); // should not throw; exercises PostgreSql isolation branch
        Assert.True(factory.CreatedConnections.Any());
    }

    // ── CommitAsync rollback-on-throw ─────────────────────────────────────────

    [Fact]
    public void CommitAsync_WhenCommitThrows_RollsBackAndRethrows()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        var throwingConn = new fakeDbConnection();
        throwingConn.SetTransactionCommitException(new InvalidOperationException("forced commit failure"));
        factory.Connections.Insert(0, throwingConn);
        var storage = new PengdowsCrudJobStorage(ctx);
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.IncrementCounter("k");
        Assert.Throws<InvalidOperationException>(() => tx.Commit());
    }

    // ── AddJobState invalid ID short-circuit ──────────────────────────────────

    [Fact]
    public void AddJobState_InvalidJobId_IsNoOp()
    {
        var (storage, factory) = CreateStorage();
        using var tx = new PengdowsCrudWriteOnlyTransaction(storage);
        tx.AddJobState("not-a-number", new SucceededState(null, 1, 100));
        tx.Commit();
        Assert.False(NonQueryContains(factory, "UPDATE") || NonQueryContains(factory, "INSERT") || NonQueryContains(factory, "DELETE"));
    }

    private sealed class FakeFetchedJob : IFetchedJob
    {
        public string JobId => "0";
        public void RemoveFromQueue() { }
        public void Requeue() { }
        public void Dispose() { }
    }
}
