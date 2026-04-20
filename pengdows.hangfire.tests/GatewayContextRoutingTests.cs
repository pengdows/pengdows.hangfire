using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using pengdows.hangfire.gateways;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Verifies that each gateway method routes SQL to the supplied IDatabaseContext override
/// rather than the root context injected at construction time.
/// </summary>
public sealed class GatewayContextRoutingTests
{
    private static (DatabaseContext Root, fakeDbFactory RootFactory,
                    DatabaseContext Override, fakeDbFactory OverrideFactory)
        MakePair(SupportedDatabase db = SupportedDatabase.SqlServer)
    {
        var rootFactory     = new fakeDbFactory(db);
        var rootCtx         = new DatabaseContext("Data Source=root",     rootFactory);
        var overrideFactory = new fakeDbFactory(db);
        var overrideCtx     = new DatabaseContext("Data Source=override", overrideFactory);
        return (rootCtx, rootFactory, overrideCtx, overrideFactory);
    }

    private static bool SqlContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedReaderTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase))
        || f.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
            .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    // ── AggregatedCounterGateway ─────────────────────────────────────────────

    [Fact]
    public async Task AggregatedCounter_GetTimelineAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new AggregatedCounterGateway(root);
            await gw.GetTimelineAsync(new[] { "k" }, over);

            Assert.True(SqlContains(overF, "AggregatedCounter"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "AggregatedCounter"), "SQL must not appear in root factory");
        }
    }

    // ── CounterGateway ───────────────────────────────────────────────────────

    [Fact]
    public async Task Counter_AggregateAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new CounterGateway(over);
            // Empty reader → returns 0 without upsert; only the SELECT from Counter runs.
            await gw.AggregateAsync(10, over);

            Assert.True(SqlContains(overF, "Counter"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "Counter"), "SQL must not appear in root factory");
        }
    }

    // ── HashGateway ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Hash_GetAllEntriesAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new HashGateway(root);
            await gw.GetAllEntriesAsync("ctx-key", over);

            Assert.True(SqlContains(overF, "Hash"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "Hash"), "SQL must not appear in root factory");
        }
    }

    // ── JobGateway ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Job_GetPagedByStateAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new JobGateway(root);
            await gw.GetPagedByStateAsync("Enqueued", 0, 10, over);

            Assert.True(SqlContains(overF, "StateName"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "StateName"), "SQL must not appear in root factory");
        }
    }

    // ── JobParameterGateway ──────────────────────────────────────────────────

    [Fact]
    public async Task JobParameter_GetAllForJobAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new JobParameterGateway(root);
            await gw.GetAllForJobAsync(1L, over);

            Assert.True(SqlContains(overF, "JobParameter"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "JobParameter"), "SQL must not appear in root factory");
        }
    }

    // ── JobQueueGateway ──────────────────────────────────────────────────────

    [Fact]
    public async Task JobQueue_FetchNextJobAsync_RoutesToOverrideContext()
    {
        // Verifies both the SELECT (outer loop) and, if a candidate exists, TryClaimAsync
        // both execute on the override context — not the root context.
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new JobQueueGateway(root);
            var result = await gw.FetchNextJobAsync(new[] { "default" }, CancellationToken.None, over);

            Assert.Null(result); // empty reader → no candidates
            Assert.True(SqlContains(overF, "FetchedAt"),  "SELECT must appear in override factory");
            Assert.False(SqlContains(rootF, "FetchedAt"), "SELECT must not appear in root factory");
        }
    }

    // ── JobStateGateway ──────────────────────────────────────────────────────

    [Fact]
    public async Task JobState_GetAllForJobAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new JobStateGateway(root);
            await gw.GetAllForJobAsync(1L, over);

            Assert.True(SqlContains(overF, "State"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "State"), "SQL must not appear in root factory");
        }
    }

    // ── ListGateway ──────────────────────────────────────────────────────────

    [Fact]
    public async Task List_GetAllAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new ListGateway(root);
            await gw.GetAllAsync("ctx-key", over);

            Assert.True(SqlContains(overF, "List"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "List"), "SQL must not appear in root factory");
        }
    }

    // ── ServerGateway ────────────────────────────────────────────────────────

    [Fact]
    public async Task Server_RemoveTimedOutAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new ServerGateway(root);
            await gw.RemoveTimedOutAsync(DateTime.UtcNow, over);

            Assert.True(SqlContains(overF, "LastHeartbeat"),  "DELETE must appear in override factory");
            Assert.False(SqlContains(rootF, "LastHeartbeat"), "DELETE must not appear in root factory");
        }
    }

    // ── SetGateway ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Set_GetAllItemsAsync_RoutesToOverrideContext()
    {
        var (root, rootF, over, overF) = MakePair();
        await using (root) await using (over)
        {
            var gw = new SetGateway(root);
            await gw.GetAllItemsAsync("ctx-key", over);

            Assert.True(SqlContains(overF, "Set"),  "SQL should appear in override factory");
            Assert.False(SqlContains(rootF, "Set"), "SQL must not appear in root factory");
        }
    }
}
