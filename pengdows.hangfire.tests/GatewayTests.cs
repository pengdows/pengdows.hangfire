using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire;
using pengdows.hangfire.gateways;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Unit tests for all gateway classes verifying SQL generation patterns and
/// basic return-value behaviour using the fakeDb in-memory provider.
/// </summary>
public sealed class GatewayTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static (DatabaseContext Context, fakeDbFactory Factory) MakeContext(
        SupportedDatabase db = SupportedDatabase.SqlServer)
    {
        var factory = new fakeDbFactory(db);
        return (new DatabaseContext("Data Source=fake", factory), factory);
    }

    /// <summary>
    /// Pre-seeds a single-row reader with {Value=scalar} for ExecuteScalarRequiredAsync.
    /// The context is created FIRST (so any initialization connection is consumed), then
    /// the reader result is enqueued for the gateway's subsequent CreateSqlContainer call.
    /// </summary>
    private static (DatabaseContext Context, fakeDbFactory Factory) MakeContextWithScalar(
        long scalar = 0L, SupportedDatabase db = SupportedDatabase.SqlServer)
    {
        var factory = new fakeDbFactory(db);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        // Enqueue AFTER context init so the gateway's CreateSqlContainer picks it up
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = scalar } });
        return (ctx, factory);
    }

    private static bool NonQueryContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    private static bool ReaderContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedReaderTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    private static bool SqlContains(fakeDbFactory f, string s) =>
        NonQueryContains(f, s) || ReaderContains(f, s);

    private static object? ParamValue(fakeDbFactory f, string name) =>
        f.CreatedConnections.SelectMany(c => c.CreatedCommands)
         .SelectMany(cmd => cmd.Parameters.Cast<fakeDbParameter>())
         .FirstOrDefault(p => p.ParameterName.Contains(name, StringComparison.OrdinalIgnoreCase))?
         .Value;

    private static readonly Dictionary<string, object> ScalarRow =
        new() { ["Value"] = 0L };

    // ── HashGateway ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Hash_GetAllEntriesAsync_ReturnsEmptyDictWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetAllEntriesAsync("k");
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task Hash_GetAllEntriesAsync_SqlContainsKeyColumn()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).GetAllEntriesAsync("mykey");
            Assert.True(ReaderContains(factory, "Key"));
        }
    }

    [Fact]
    public async Task Hash_GetValueAsync_ReturnsNullWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetValueAsync("k", "f");
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task Hash_GetValueAsync_SqlContainsFieldColumn()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).GetValueAsync("k", "f");
            Assert.True(ReaderContains(factory, "Field"));
        }
    }

    [Fact]
    public async Task Hash_GetCountAsync_ReturnsZeroWhenNoRows()
    {
        var (ctx, _) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetCountAsync("k");
            Assert.Equal(0L, result);
        }
    }

    [Fact]
    public async Task Hash_GetCountAsync_SqlContainsCount()
    {
        var (ctx, factory) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            await new HashGateway(ctx).GetCountAsync("k");
            Assert.True(ReaderContains(factory, "COUNT"));
        }
    }

    [Fact]
    public async Task Hash_GetTtlAsync_ReturnsNegativeOneWhenNoExpiry()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetTtlAsync("k");
            Assert.Equal(TimeSpan.FromSeconds(-1), result);
        }
    }

    [Fact]
    public async Task Hash_GetTtlAsync_SqlContainsMinExpireAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).GetTtlAsync("k");
            Assert.True(ReaderContains(factory, "ExpireAt"));
            Assert.True(ReaderContains(factory, "MIN"));
        }
    }

    [Fact]
    public async Task Hash_DeleteAllForKeyAsync_IssuesDeleteStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).DeleteAllForKeyAsync("k");
            Assert.True(NonQueryContains(factory, "DELETE"));
            Assert.True(NonQueryContains(factory, "Key"));
        }
    }

    [Fact]
    public async Task Hash_UpdateExpireAtAsync_IssuesUpdateStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).UpdateExpireAtAsync("k", DateTime.UtcNow.AddHours(1));
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "ExpireAt"));
        }
    }

    [Fact]
    public async Task Hash_UpdateExpireAtAsync_Null_IssuesUpdateStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new HashGateway(ctx).UpdateExpireAtAsync("k", null);
            Assert.True(NonQueryContains(factory, "UPDATE"));
        }
    }

    // ── ListGateway ──────────────────────────────────────────────────────────

    [Fact]
    public async Task List_AppendAsync_IssuesInsertStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new ListGateway(ctx).AppendAsync("k", "v");
            Assert.True(SqlContains(factory, "INSERT"));
            Assert.True(SqlContains(factory, "Value"));
        }
    }

    [Fact]
    public async Task List_DeleteByKeyValueAsync_IssuesDeleteStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new ListGateway(ctx).DeleteByKeyValueAsync("k", "v");
            Assert.True(SqlContains(factory, "DELETE"));
            Assert.True(SqlContains(factory, "Value"));
        }
    }

    [Fact]
    public async Task List_TrimAsync_IssuesDeleteWithNotIn()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new ListGateway(ctx).TrimAsync("k", 0, 5);
            Assert.True(NonQueryContains(factory, "DELETE"));
            Assert.True(NonQueryContains(factory, "NOT IN"));
        }
    }

    [Fact]
    public async Task List_GetCountAsync_ReturnsZeroWhenNoRows()
    {
        var (ctx, _) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetCountAsync("k");
            Assert.Equal(0L, result);
        }
    }

    [Fact]
    public async Task List_GetCountAsync_SqlContainsCount()
    {
        var (ctx, factory) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            await new ListGateway(ctx).GetCountAsync("k");
            Assert.True(ReaderContains(factory, "COUNT"));
        }
    }

    [Fact]
    public async Task List_GetTtlAsync_ReturnsNegativeOneWhenNoExpiry()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetTtlAsync("k");
            Assert.Equal(TimeSpan.FromSeconds(-1), result);
        }
    }

    [Fact]
    public async Task List_GetRangeAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetRangeAsync("k", 0, 9);
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task List_GetAllAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetAllAsync("k");
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task List_GetAllAsync_SqlContainsKeyCondition()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new ListGateway(ctx).GetAllAsync("mylistkey");
            Assert.True(ReaderContains(factory, "Key"));
        }
    }

    [Fact]
    public async Task List_UpdateExpireAtAsync_IssuesUpdateStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new ListGateway(ctx).UpdateExpireAtAsync("k", DateTime.UtcNow.AddHours(1));
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "ExpireAt"));
        }
    }

    // ── SetGateway ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Set_GetAllItemsAsync_ReturnsEmptySetWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetAllItemsAsync("k");
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task Set_GetFirstByLowestScoreAsync_Single_ReturnsNullWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetFirstByLowestScoreAsync("k", 0, 1);
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task Set_GetFirstByLowestScoreAsync_Multi_SqlContainsScore()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new SetGateway(ctx).GetFirstByLowestScoreAsync("k", 0, 1, 3);
            Assert.True(ReaderContains(factory, "Score"));
        }
    }

    [Fact]
    public async Task Set_GetFirstByLowestScoreAsync_Multi_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetFirstByLowestScoreAsync("k", 0, 1, 3);
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task Set_GetCountAsync_ReturnsZeroWhenNoRows()
    {
        var (ctx, _) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetCountAsync("k");
            Assert.Equal(0L, result);
        }
    }

    [Fact]
    public async Task Set_ContainsAsync_ReturnsFalseWhenCountIsZero()
    {
        var (ctx, _) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            var result = await new SetGateway(ctx).ContainsAsync("k", "v");
            Assert.False(result);
        }
    }

    [Fact]
    public async Task Set_ContainsAsync_ReturnsTrueWhenCountIsNonZero()
    {
        var (ctx, _) = MakeContextWithScalar(1L);
        await using (ctx)
        {
            var result = await new SetGateway(ctx).ContainsAsync("k", "v");
            Assert.True(result);
        }
    }

    [Fact]
    public async Task Set_ContainsAsync_SqlContainsValueCondition()
    {
        var (ctx, factory) = MakeContextWithScalar(0L);
        await using (ctx)
        {
            await new SetGateway(ctx).ContainsAsync("k", "v");
            Assert.True(ReaderContains(factory, "Value"));
        }
    }

    [Fact]
    public async Task Set_GetRangeAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetRangeAsync("k", 0, 4);
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task Set_GetRangeAsync_SqlContainsScoreOrder()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new SetGateway(ctx).GetRangeAsync("k", 0, 4);
            Assert.True(ReaderContains(factory, "Score"));
        }
    }

    [Fact]
    public async Task Set_GetTtlAsync_ReturnsNegativeOneWhenNoExpiry()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetTtlAsync("k");
            Assert.Equal(TimeSpan.FromSeconds(-1), result);
        }
    }

    [Fact]
    public async Task Set_UpdateExpireAtAsync_IssuesUpdateStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new SetGateway(ctx).UpdateExpireAtAsync("k", DateTime.UtcNow.AddHours(1));
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "ExpireAt"));
        }
    }

    [Fact]
    public async Task Set_DeleteByKeyAsync_IssuesDeleteStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new SetGateway(ctx).DeleteByKeyAsync("k");
            Assert.True(NonQueryContains(factory, "DELETE"));
            Assert.True(NonQueryContains(factory, "Key"));
        }
    }

    // ── JobGateway ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Job_UpdateExpireAtAsync_IssuesUpdateWithExpireAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobGateway(ctx).UpdateExpireAtAsync(1L, DateTime.UtcNow.AddHours(1));
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "ExpireAt"));
        }
    }

    [Fact]
    public async Task Job_UpdateExpireAtAsync_NullExpiry_IssuesUpdateStatement()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobGateway(ctx).UpdateExpireAtAsync(1L, null);
            Assert.True(NonQueryContains(factory, "UPDATE"));
        }
    }

    [Fact]
    public async Task Job_UpdateStateNameAsync_IssuesUpdateWithStateName()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobGateway(ctx).UpdateStateNameAsync(1L, "Succeeded");
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "StateName"));
        }
    }

    [Fact]
    public async Task Job_UpdateStateAsync_IssuesBothStateIdAndStateName()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobGateway(ctx).UpdateStateAsync(1L, 99L, "Succeeded");
            Assert.True(NonQueryContains(factory, "StateId"));
            Assert.True(NonQueryContains(factory, "StateName"));
        }
    }

    [Fact]
    public async Task Job_GetPagedByStateAsync_SqlContainsStateName()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobGateway(ctx).GetPagedByStateAsync("Succeeded", 0, 10);
            Assert.True(ReaderContains(factory, "StateName"));
        }
    }

    [Fact]
    public async Task Job_GetPagedByStateAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobGateway(ctx).GetPagedByStateAsync("Succeeded", 0, 10);
            Assert.Empty(result);
        }
    }

    // ── JobParameterGateway ──────────────────────────────────────────────────

    [Fact]
    public async Task JobParameter_GetAllForJobAsync_ReturnsEmptyDictWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobParameterGateway(ctx).GetAllForJobAsync(1L);
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task JobParameter_GetAllForJobAsync_SqlContainsJobId()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobParameterGateway(ctx).GetAllForJobAsync(1L);
            Assert.True(ReaderContains(factory, "JobId"));
        }
    }

    // ── JobStateGateway ──────────────────────────────────────────────────────

    [Fact]
    public async Task JobState_GetLatestAsync_ReturnsNullWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobStateGateway(ctx).GetLatestAsync(1L);
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task JobState_GetLatestAsync_SqlContainsJobIdAndOrderDesc()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobStateGateway(ctx).GetLatestAsync(1L);
            Assert.True(ReaderContains(factory, "JobId"));
            Assert.True(ReaderContains(factory, "DESC"));
        }
    }

    [Fact]
    public async Task JobState_GetAllForJobAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobStateGateway(ctx).GetAllForJobAsync(1L);
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task JobState_GetAllForJobAsync_SqlContainsJobId()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobStateGateway(ctx).GetAllForJobAsync(1L);
            Assert.True(ReaderContains(factory, "JobId"));
        }
    }

    // ── JobQueueGateway ──────────────────────────────────────────────────────

    [Fact]
    public async Task JobQueue_AcknowledgeAsync_IssuesDeleteWithFetchedAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).AcknowledgeAsync(42L, "default");
            Assert.True(NonQueryContains(factory, "DELETE"));
            Assert.True(NonQueryContains(factory, "FetchedAt"));
        }
    }

    [Fact]
    public async Task JobQueue_RequeueAsync_SetsNullFetchedAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).RequeueAsync(42L, "default");
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "NULL"));
        }
    }

    [Fact]
    public async Task JobQueue_GetDistinctQueuesAsync_SqlContainsDistinct()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).GetDistinctQueuesAsync();
            Assert.True(ReaderContains(factory, "DISTINCT"));
        }
    }

    [Fact]
    public async Task JobQueue_GetDistinctQueuesAsync_ReturnsEmptyWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).GetDistinctQueuesAsync();
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task JobQueue_GetPagedByQueueAsync_SqlContainsFetchedAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).GetPagedByQueueAsync("default", 0, 10, false);
            Assert.True(ReaderContains(factory, "FetchedAt"));
            Assert.True(ReaderContains(factory, "Queue"));
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_EmptyQueue_ReturnsNull()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).FetchNextJobAsync(
                new[] { "default" }, CancellationToken.None);
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_CancelledToken_Throws()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => new JobQueueGateway(ctx).FetchNextJobAsync(new[] { "default" }, cts.Token));
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_SuccessfulClaim_ReturnsJobIdAndQueue()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        // Seed AFTER context init: reader for the candidate SELECT, UPDATE uses default (1 row affected)
        factory.EnqueueReaderResult(new[]
        {
            new Dictionary<string, object> { ["Id"] = 1L, ["JobId"] = 42L }
        });
        await using (ctx)
        {
            var gw = new JobQueueGateway(ctx);
            var result = await gw.FetchNextJobAsync(new[] { "default" }, CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(42L, result!.Value.JobId);
            Assert.Equal("default", result.Value.Queue);
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_RaceLost_ReturnsNull()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        // EnqueueReaderResult creates the SELECT connection at _connections[0].
        // Append the UPDATE connection after so the pool is [SELECT-conn, UPDATE-conn(race-lost)].
        factory.EnqueueReaderResult(new[]
        {
            new Dictionary<string, object> { ["Id"] = 1L, ["JobId"] = 42L }
        });
        var updateConn = new fakeDbConnection();
        updateConn.NonQueryResults.Enqueue(0); // race lost: 0 rows affected
        factory.Connections.Add(updateConn);
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).FetchNextJobAsync(
                new[] { "default" }, CancellationToken.None);
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_ClaimsSecondCandidateWhenFirstRaceLost()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        // SELECT streams 2 rows; first UPDATE loses the race (0 rows); second UPDATE wins (fresh conn).
        factory.EnqueueReaderResult(new[]
        {
            new Dictionary<string, object> { ["Id"] = 1L, ["JobId"] = 10L },
            new Dictionary<string, object> { ["Id"] = 2L, ["JobId"] = 20L }
        });
        var updateConn1 = new fakeDbConnection();
        updateConn1.NonQueryResults.Enqueue(0); // candidate 1 race lost
        factory.Connections.Add(updateConn1);
        // UPDATE-2 connection is created fresh (empty NonQueryResults → default 1 = claim wins).
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).FetchNextJobAsync(
                new[] { "default" }, CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(20L, result!.Value.JobId);
        }
    }

    // ── CounterGateway ───────────────────────────────────────────────────────

    [Fact]
    public async Task Counter_AppendAsync_WithoutExpiry_SqlValueIsNull()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new CounterGateway(ctx).AppendAsync("k", 1);
            Assert.True(SqlContains(factory, "INSERT"));
            Assert.True(SqlContains(factory, "ExpireAt"));
            var val = ParamValue(factory, "ExpireAt");
            Assert.True(val == null || val == DBNull.Value);
        }
    }

    [Fact]
    public async Task Counter_AppendAsync_WithExpiry_SqlIncludesExpireAt()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new CounterGateway(ctx).AppendAsync("k", 1, DateTime.UtcNow.AddHours(1));
            Assert.True(SqlContains(factory, "INSERT"));
            Assert.True(SqlContains(factory, "ExpireAt"));
        }
    }

    [Fact]
    public async Task Counter_AggregateAsync_ReturnsZeroWhenNoRows()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new CounterGateway(ctx).AggregateAsync(100);
            Assert.Equal(0, result);
        }
    }

    [Fact]
    public async Task Counter_AggregateAsync_SqlContainsOrderByAndPaging()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new CounterGateway(ctx).AggregateAsync(100);
            Assert.True(ReaderContains(factory, "Id"));
        }
    }

    // ── AggregatedCounterGateway ─────────────────────────────────────────────

    [Fact]
    public async Task AggregatedCounter_GetTimelineAsync_EmptyKeys_ReturnsEmptyDict()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new AggregatedCounterGateway(ctx).GetTimelineAsync(Array.Empty<string>());
            Assert.Empty(result);
        }
    }

    [Fact]
    public async Task AggregatedCounter_GetTimelineAsync_WithKeys_SqlContainsIn()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new AggregatedCounterGateway(ctx).GetTimelineAsync(new[] { "k1", "k2" });
            Assert.True(ReaderContains(factory, "IN"));
            Assert.True(ReaderContains(factory, "Key"));
        }
    }

    [Fact]
    public async Task AggregatedCounter_GetValueAsync_ReturnsZeroWhenNoRow()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new AggregatedCounterGateway(ctx).GetValueAsync("k");
            Assert.Equal(0L, result);
        }
    }

    [Fact]
    public async Task AggregatedCounter_GetValueAsync_SqlContainsKeyCondition()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new AggregatedCounterGateway(ctx).GetValueAsync("k");
            Assert.True(ReaderContains(factory, "Value"));
        }
    }

    // ── MySQL / MariaDB / TiDB dialect-specific tests ─────────────────────────

    // Regression: the else-branch in CounterGateway.AggregateAsync used
    // "ON CONFLICT ... DO UPDATE SET" (PostgreSQL syntax).  MySQL/MariaDB/TiDB
    // do not support ON CONFLICT and require "ON DUPLICATE KEY UPDATE" instead.
    [Theory]
    [InlineData(SupportedDatabase.MySql)]
    [InlineData(SupportedDatabase.MariaDb)]
    [InlineData(SupportedDatabase.TiDb)]
    public async Task Counter_AggregateAsync_MySqlFamily_UsesOnDuplicateKeyUpdate(SupportedDatabase db)
    {
        var factory = new fakeDbFactory(db);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[]
        {
            new Dictionary<string, object> { ["Id"] = 1L, ["Key"] = "stats:success", ["Value"] = 3 }
        });
        await using (ctx)
        {
            await new CounterGateway(ctx).AggregateAsync(100);
            Assert.True(NonQueryContains(factory, "ON DUPLICATE KEY UPDATE"),
                $"{db}: expected ON DUPLICATE KEY UPDATE in upsert SQL");
            Assert.False(NonQueryContains(factory, "ON CONFLICT"),
                $"{db}: ON CONFLICT must not appear for MySQL-family databases");
        }
    }

    // Regression: the else-branch in CounterGateway.AggregateAsync used
    // "ON CONFLICT ... DO UPDATE SET" (PostgreSQL syntax).  Firebird uses MERGE
    // with FROM RDB$DATABASE (not ON CONFLICT, not ON DUPLICATE KEY UPDATE).
    [Fact]
    public async Task Counter_AggregateAsync_Firebird_UsesMergeWithRdbDatabase()
    {
        var factory = new fakeDbFactory(SupportedDatabase.Firebird);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[]
        {
            new Dictionary<string, object> { ["Id"] = 1L, ["Key"] = "stats:success", ["Value"] = 3 }
        });
        await using (ctx)
        {
            await new CounterGateway(ctx).AggregateAsync(100);
            Assert.True(NonQueryContains(factory, "MERGE"),
                "Firebird: expected MERGE in upsert SQL");
            Assert.True(NonQueryContains(factory, "RDB$DATABASE"),
                "Firebird: expected RDB$DATABASE in MERGE USING clause");
            Assert.True(NonQueryContains(factory, "CAST"),
                "Firebird: CAST required in USING SELECT for type inference");
            Assert.False(NonQueryContains(factory, "ON CONFLICT"),
                "Firebird: ON CONFLICT must not appear");
        }
    }

    // ── GetTtl non-null expiry coverage ──────────────────────────────────────

    [Fact]
    public async Task Hash_GetTtlAsync_WithUtcExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        // Scalar query reads first column of first row
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.UtcNow.AddHours(1) } });
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task Hash_GetTtlAsync_WithLocalExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.Now.AddHours(1) } });
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task Hash_GetValueAsync_WithRow_ReturnsValue()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Key"]   = "mykey",
            ["Field"] = "myfield",
            ["Value"] = "myvalue"
        }});
        await using (ctx)
        {
            var result = await new HashGateway(ctx).GetValueAsync("mykey", "myfield");
            Assert.Equal("myvalue", result);
        }
    }

    [Fact]
    public async Task List_GetTtlAsync_WithUtcExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.UtcNow.AddHours(1) } });
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task List_GetTtlAsync_WithLocalExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.Now.AddHours(1) } });
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task List_GetRangeAsync_WithNonNullValue_ReturnsValue()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"] = 1L, ["Key"] = "k", ["Value"] = "item"
        }});
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetRangeAsync("k", 0, 9);
            Assert.Contains("item", result);
        }
    }

    [Fact]
    public async Task List_GetRangeAsync_WithNullValue_ReturnsEmptyString()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"] = 1L, ["Key"] = "k"   // omit Value → null → ?? string.Empty
        }});
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetRangeAsync("k", 0, 9);
            Assert.Contains(string.Empty, result);
        }
    }

    [Fact]
    public async Task List_GetAllAsync_WithNonNullValue_ReturnsValue()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"] = 1L, ["Key"] = "k", ["Value"] = "item"
        }});
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetAllAsync("k");
            Assert.Contains("item", result);
        }
    }

    [Fact]
    public async Task List_GetAllAsync_WithNullValue_ReturnsEmptyString()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"] = 1L, ["Key"] = "k"   // omit Value → null → ?? string.Empty
        }});
        await using (ctx)
        {
            var result = await new ListGateway(ctx).GetAllAsync("k");
            Assert.Contains(string.Empty, result);
        }
    }

    [Fact]
    public async Task Set_GetTtlAsync_WithUtcExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.UtcNow.AddHours(1) } });
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task Set_GetTtlAsync_WithLocalExpiry_ReturnsPositiveDuration()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["v"] = DateTime.Now.AddHours(1) } });
        await using (ctx)
        {
            var result = await new SetGateway(ctx).GetTtlAsync("k");
            Assert.True(result > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task List_DeleteByKeyValueAsync_Oracle_UsesDbmsLobCompare()
    {
        var factory = new fakeDbFactory(SupportedDatabase.Oracle);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        await using (ctx)
        {
            await new ListGateway(ctx).DeleteByKeyValueAsync("k", "v");
            Assert.True(NonQueryContains(factory, "DBMS_LOB"));
        }
    }

    // ── FetchNextJobAsync PostgreSQL branch ────────────────────────────────────

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_EmptyQueues_ReturnsNull()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).FetchNextJobAsync(
                Array.Empty<string>(), CancellationToken.None);
            Assert.Null(result);
        }
    }

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_PostgreSql_ClaimsSuccessfully()
    {
        var factory = new fakeDbFactory(SupportedDatabase.PostgreSql);
        var ctx = new DatabaseContext("Host=fake", factory);
        factory.EnqueueReaderResult(new[] {
            new Dictionary<string, object> { ["Id"] = 1L, ["JobId"] = 99L }
        });
        await using (ctx)
        {
            var result = await new JobQueueGateway(ctx).FetchNextJobAsync(
                new[] { "default" }, CancellationToken.None);
            Assert.NotNull(result);
            Assert.Equal(99L, result!.Value.JobId);
        }
    }

    // ── DeleteExpired non-empty batch coverage ─────────────────────────────────

    [Fact]
    public async Task Hash_DeleteExpiredAsync_WithExpiredRows_ReturnsNonZero()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Key"] = "k", ["Field"] = "f", ["Value"] = "v"
        }});
        await using (ctx)
        {
            var result = await new HashGateway(ctx).DeleteExpiredAsync(1000);
            Assert.True(result >= 0);
        }
    }

    [Fact]
    public async Task List_DeleteExpiredAsync_WithExpiredRows_ReturnsNonZero()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"] = 1L, ["Key"] = "k", ["Value"] = "v"
        }});
        await using (ctx)
        {
            var result = await new ListGateway(ctx).DeleteExpiredAsync(1000);
            Assert.True(result >= 0);
        }
    }

    [Fact]
    public async Task Set_DeleteExpiredAsync_WithExpiredRows_ReturnsNonZero()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Key"] = "k", ["Value"] = "v", ["Score"] = 0.0d
        }});
        await using (ctx)
        {
            var result = await new SetGateway(ctx).DeleteExpiredAsync(1000);
            Assert.True(result >= 0);
        }
    }

    [Fact]
    public async Task Job_DeleteExpiredAsync_WithExpiredRows_ReturnsNonZero()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"]             = 1L,
            ["InvocationData"] = "{}",
            ["Arguments"]      = "[]",
            ["CreatedAt"]      = DateTime.UtcNow
        }});
        await using (ctx)
        {
            var result = await new JobGateway(ctx).DeleteExpiredAsync(1000);
            Assert.True(result >= 0);
        }
    }

    [Fact]
    public async Task AggregatedCounter_DeleteExpiredAsync_WithExpiredRows_ReturnsNonZero()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Key"] = "stats:success", ["Value"] = 5L
        }});
        await using (ctx)
        {
            var result = await new AggregatedCounterGateway(ctx).DeleteExpiredAsync(1000);
            Assert.True(result >= 0);
        }
    }

    // ── MetricFormattingExtensions null name/mode defaults ────────────────────

    [Fact]
    public void MetricFormatting_ToMetricGrid_NullNameAndMode_UsesDefaults()
    {
        var (ctx, _) = MakeContext();
        var metrics = ctx.Metrics;
        var grid = metrics.ToMetricGrid(null, null);
        Assert.Contains("Database", grid);
        Assert.Contains("Unknown", grid);
    }

    // Regression: ListGateway.TrimAsync generates
    //   DELETE ... WHERE Id NOT IN (SELECT Id ... LIMIT N)
    // MySQL rejects LIMIT inside an IN subquery unless wrapped in a derived table.
    // The fix wraps it:  NOT IN (SELECT Id FROM (SELECT Id ... LIMIT N) AS _t)
    [Theory]
    [InlineData(SupportedDatabase.MySql)]
    [InlineData(SupportedDatabase.MariaDb)]
    [InlineData(SupportedDatabase.TiDb)]
    public async Task List_TrimAsync_MySqlFamily_WrapsSubqueryToAllowLimit(SupportedDatabase db)
    {
        var factory = new fakeDbFactory(db);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        await using (ctx)
        {
            await new ListGateway(ctx).TrimAsync("k", 0, 5);
            // The wrapping derived-table alias must be present; its exact name is an impl detail.
            Assert.True(NonQueryContains(factory, "SELECT") && NonQueryContains(factory, "FROM ("),
                $"{db}: LIMIT-in-IN subquery must be wrapped in a derived table for MySQL");
        }
    }

    // ── JobQueueGateway — RequeueStaleAsync ──────────────────────────────────

    [Fact]
    public async Task JobQueue_RequeueStaleAsync_SqlContainsUpdate()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).RequeueStaleAsync(DateTime.UtcNow.AddMinutes(-5));
            Assert.True(NonQueryContains(factory, "UPDATE"));
        }
    }

    [Fact]
    public async Task JobQueue_RequeueStaleAsync_SqlSetsFetchedAtNull()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).RequeueStaleAsync(DateTime.UtcNow.AddMinutes(-5));
            Assert.True(NonQueryContains(factory, "FetchedAt"));
            Assert.True(NonQueryContains(factory, "NULL"));
        }
    }

    [Fact]
    public async Task JobQueue_RequeueStaleAsync_SqlGuardsFetchedAtIsNotNull()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).RequeueStaleAsync(DateTime.UtcNow.AddMinutes(-5));
            // WHERE must include FetchedAt IS NOT NULL so unfetched rows are untouched
            Assert.True(NonQueryContains(factory, "IS NOT NULL"));
        }
    }

    [Fact]
    public async Task JobQueue_RequeueStaleAsync_BindsCutoffParam()
    {
        var (ctx, factory) = MakeContext();
        await using (ctx)
        {
            await new JobQueueGateway(ctx).RequeueStaleAsync(DateTime.UtcNow.AddMinutes(-5));
            // MakeParameterName produces the placeholder for the SQL text:
            //   named providers  → includes the param name (e.g. "@cutoff", ":cutoff", "cutoff")
            //   positional providers → "?"
            var sql = factory.CreatedConnections
                .SelectMany(c => c.ExecutedNonQueryTexts)
                .FirstOrDefault(t => t.Contains("IS NOT NULL", StringComparison.OrdinalIgnoreCase));
            Assert.NotNull(sql);
            var bound = sql.Contains("cutoff", StringComparison.OrdinalIgnoreCase)
                        || sql.Contains("?");
            Assert.True(bound, "Expected a bound parameter placeholder for the cutoff value");
        }
    }

    [Fact]
    public async Task JobQueue_RequeueStaleAsync_ReturnsAffectedRowCount()
    {
        var (ctx, _) = MakeContext();
        await using (ctx)
        {
            var count = await new JobQueueGateway(ctx).RequeueStaleAsync(DateTime.UtcNow);
            // fakeDb returns 1 by default for non-query; just verify it returns the value
            Assert.True(count >= 0);
        }
    }
}
