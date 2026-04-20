using System.Reflection;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;
using Hangfire.Storage.Monitoring;
using Hangfire.States;
using pengdows.hangfire;

namespace pengdows.hangfire.tests;

public sealed class MonitoringApiTests
{
    private static (PengdowsCrudMonitoringApi Api, fakeDbFactory Factory) CreateApi()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var context = new DatabaseContext("Data Source=fake;EmulatedProduct=SqlServer", factory);
        var storage = new PengdowsCrudJobStorage(context);
        return (new PengdowsCrudMonitoringApi(storage), factory);
    }

    [Fact]
    public void TruncateToHour_Correctness()
    {
        var method = typeof(PengdowsCrudMonitoringApi).GetMethod("TruncateToHour", BindingFlags.NonPublic | BindingFlags.Static)!;
        var input  = new DateTime(2024, 6, 15, 14, 37, 22, 500, DateTimeKind.Utc);
        var result = (DateTime)method.Invoke(null, [input])!;
        Assert.Equal(new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void GetDatabaseMetrics_ReturnsMainDb()
    {
        var (api, _) = CreateApi();
        var metrics = api.GetDatabaseMetrics();
        Assert.NotEmpty(metrics);
    }

    [Fact]
    public void GetDatabaseMetricGrid_ReturnsNonEmptyString()
    {
        var (api, _) = CreateApi();
        var grid = api.GetDatabaseMetricGrid();
        Assert.Contains("Metrics Grid", grid);
    }

    [Fact]
    public void GetStatistics_IssuesExpectedQueries()
    {
        var (api, factory) = CreateApi();
        for (int i = 0; i < 8; i++)
        {
            factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = (long)i } });
        }

        var stats = api.GetStatistics();
        Assert.NotNull(stats);
    }

    [Fact]
    public void Queues_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { 
            new Dictionary<string, object> { ["Queue"] = "default" }
        });
        
        // CountLength
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 10L } });
        // CountFetched
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 5L } });
        
        // EnqueuedJobs calls Count then SELECT
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });

        var result = api.Queues();
        Assert.Single(result);
        Assert.Equal("default", result[0].Name);
    }

    [Fact]
    public void Servers_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { 
            new Dictionary<string, object> { ["Id"] = "s1", ["Data"] = "{}", ["LastHeartbeat"] = DateTime.UtcNow }
        });
        var result = api.Servers();
        Assert.Single(result);
    }

    [Fact]
    public void SucceededByDatesCount_SumsValuesFromAggregatedCounter()
    {
        var (api, factory) = CreateApi();
        var rows = new List<Dictionary<string, object>>();
        for (int i = 0; i < 7; i++)
        {
            rows.Add(new Dictionary<string, object> { ["Key"] = "key" + i, ["Value"] = (long)i });
        }
        factory.EnqueueReaderResult(rows);

        var result = api.SucceededByDatesCount();
        Assert.Equal(7, result.Count);
    }

    [Fact]
    public void JobDetails_ReturnsFullJobInfo()
    {
        var (api, factory) = CreateApi();
        var now = DateTime.UtcNow;
        var hfJob = new Hangfire.Common.Job(typeof(GC), typeof(GC).GetMethod("Collect", Array.Empty<Type>())!);

        // Each of the 3 queries (job, states, params) creates its own ephemeral connection.
        // factory.Connections is LIFO via Insert(0), so add in reverse order of use.
        // params (3rd query) → inserted first, ends up at back
        var cParam = new fakeDbConnection();
        cParam.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["JobId"] = (object?)1L, ["Name"] = "p1", ["Value"] = "v1"
        }});
        factory.Connections.Insert(0, cParam);

        // states (2nd query) → inserted second
        var cState = new fakeDbConnection();
        cState.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)100L, ["JobId"] = 1L, ["Name"] = "Succeeded",
            ["Reason"] = "Done", ["CreatedAt"] = now, ["Data"] = "{}"
        }});
        factory.Connections.Insert(0, cState);

        // job (1st query) → inserted last, ends up at front
        var cJob = new fakeDbConnection();
        cJob.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["StateId"] = 100L, ["StateName"] = "Succeeded",
            ["InvocationData"] = pengdows.hangfire.JsonHelper.Serialize(Hangfire.Storage.InvocationData.SerializeJob(hfJob)),
            ["Arguments"] = "[]", ["CreatedAt"] = now, ["ExpireAt"] = now
        }});
        factory.Connections.Insert(0, cJob);

        var result = api.JobDetails("1");
        Assert.NotNull(result);
        Assert.NotEmpty(result.History);
    }

    [Fact]
    public void EnqueuedJobs_EmptyQueue_ReturnsEmptyList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        var result = api.EnqueuedJobs("default", 0, 10);
        Assert.Empty(result);
    }

    [Fact]
    public void FetchedJobs_EmptyQueue_ReturnsEmptyList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        var result = api.FetchedJobs("default", 0, 10);
        Assert.Empty(result);
    }

    [Fact]
    public void FetchedJobs_NoQueueItems_HitsEarlyReturnPath()
    {
        var (api, _) = CreateApi();
        // No reader enqueued → GetPagedByQueueAsync returns empty list → early return at queueItems.Count == 0
        var result = api.FetchedJobs("default", 0, 10);
        Assert.Empty(result);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string MakeInvocationData()
    {
        var hfJob = new Hangfire.Common.Job(typeof(GC), typeof(GC).GetMethod("Collect", Array.Empty<Type>())!);
        return JsonHelper.Serialize(Hangfire.Storage.InvocationData.SerializeJob(hfJob));
    }

    private static Dictionary<string, object> MakeJobRow(string stateName) => new()
    {
        ["Id"]             = 1L,
        ["StateId"]        = 1L,
        ["StateName"]      = stateName,
        ["InvocationData"] = MakeInvocationData(),
        ["Arguments"]      = "[]",
        ["CreatedAt"]      = DateTime.UtcNow,
        ["ExpireAt"]       = DateTime.UtcNow.AddHours(1),
    };

    // ── JobDetails edge cases ─────────────────────────────────────────────────

    [Fact]
    public void JobDetails_InvalidId_ReturnsNull()
    {
        var (api, _) = CreateApi();
        var result = api.JobDetails("not-an-id");
        Assert.Null(result);
    }

    [Fact]
    public void JobDetails_NotFound_ReturnsNull()
    {
        var (api, _) = CreateApi();
        // No reader set up → RetrieveOneAsync returns null
        var result = api.JobDetails("999");
        Assert.Null(result);
    }

    // ── Paged state list methods ──────────────────────────────────────────────

    [Fact]
    public void ProcessingJobs_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(ProcessingState.StateName) });
        var result = api.ProcessingJobs(0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void ScheduledJobs_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(ScheduledState.StateName) });
        var result = api.ScheduledJobs(0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void SucceededJobs_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(SucceededState.StateName) });
        var result = api.SucceededJobs(0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void FailedJobs_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(FailedState.StateName) });
        var result = api.FailedJobs(0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void DeletedJobs_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(DeletedState.StateName) });
        var result = api.DeletedJobs(0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void AwaitingJobs_EmptyResult_ReturnsEmptyList()
    {
        var (api, _) = CreateApi();
        var result = api.AwaitingJobs(0, 10);
        Assert.Empty(result);
    }

    [Fact]
    public void AwaitingJobs_WithJob_ReturnsPopulatedList()
    {
        var (api, factory) = CreateApi();
        factory.EnqueueReaderResult(new[] { MakeJobRow(AwaitingState.StateName) });
        var result = api.AwaitingJobs(0, 10);
        Assert.Single(result);
    }

    // ── Count methods ─────────────────────────────────────────────────────────

    [Fact]
    public void AwaitingCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.AwaitingCount() >= 0);
    }

    [Fact]
    public void ScheduledCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.ScheduledCount() >= 0);
    }

    [Fact]
    public void FailedCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.FailedCount() >= 0);
    }

    [Fact]
    public void ProcessingCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.ProcessingCount() >= 0);
    }

    [Fact]
    public void SucceededListCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.SucceededListCount() >= 0);
    }

    [Fact]
    public void DeletedListCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.DeletedListCount() >= 0);
    }

    [Fact]
    public void EnqueuedCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.EnqueuedCount("default") >= 0);
    }

    [Fact]
    public void FetchedCount_ReturnsNonNegative()
    {
        var (api, _) = CreateApi();
        Assert.True(api.FetchedCount("default") >= 0);
    }

    // ── Timeline methods ──────────────────────────────────────────────────────

    [Fact]
    public void HourlySucceededJobs_Returns24Slots()
    {
        var (api, _) = CreateApi();
        var result = api.HourlySucceededJobs();
        Assert.Equal(24, result.Count);
    }

    [Fact]
    public void HourlyFailedJobs_Returns24Slots()
    {
        var (api, _) = CreateApi();
        var result = api.HourlyFailedJobs();
        Assert.Equal(24, result.Count);
    }

    [Fact]
    public void FailedByDatesCount_Returns7Slots()
    {
        var (api, _) = CreateApi();
        var result = api.FailedByDatesCount();
        Assert.Equal(7, result.Count);
    }

    // ── Non-empty EnqueuedJobs / FetchedJobs ─────────────────────────────────

    [Fact]
    public void EnqueuedJobs_NonEmptyQueue_ReturnsItems()
    {
        var (api, factory) = CreateApi();
        var invData = MakeInvocationData();
        var now     = DateTime.UtcNow;

        // RetrieveAsync (2nd CreateConnection) — insert before queue connection
        var cJob = new fakeDbConnection();
        cJob.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["StateId"] = 1L, ["StateName"] = "Enqueued",
            ["InvocationData"] = invData, ["Arguments"] = "[]",
            ["CreatedAt"] = now, ["ExpireAt"] = (object?)now.AddHours(1)
        }});
        factory.Connections.Insert(0, cJob);

        // GetPagedByQueueAsync (1st CreateConnection) — at front of pool
        // fakeDb cannot infer type from null; use a sentinel timestamp (the WHERE IS NULL is not enforced by fakeDb)
        var cQueue = new fakeDbConnection();
        cQueue.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["JobId"] = 1L, ["Queue"] = "default",
            ["FetchedAt"] = (object?)DateTime.MinValue
        }});
        factory.Connections.Insert(0, cQueue);

        var result = api.EnqueuedJobs("default", 0, 10);
        Assert.Single(result);
    }

    [Fact]
    public void FetchedJobs_NonEmptyQueue_ReturnsItems()
    {
        var (api, factory) = CreateApi();
        var invData  = MakeInvocationData();
        var now      = DateTime.UtcNow;

        // RetrieveAsync (2nd CreateConnection)
        var cJob = new fakeDbConnection();
        cJob.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["StateId"] = 1L, ["StateName"] = "Processing",
            ["InvocationData"] = invData, ["Arguments"] = "[]",
            ["CreatedAt"] = now, ["ExpireAt"] = (object?)now.AddHours(1)
        }});
        factory.Connections.Insert(0, cJob);

        // GetPagedByQueueAsync fetched=true (1st CreateConnection)
        var cQueue = new fakeDbConnection();
        cQueue.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["JobId"] = 1L, ["Queue"] = "default",
            ["FetchedAt"] = (object?)now
        }});
        factory.Connections.Insert(0, cQueue);

        var result = api.FetchedJobs("default", 0, 10);
        Assert.Single(result);
    }

    // ── Constructor null guard ────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new PengdowsCrudMonitoringApi(null!));
    }

    // ── Additional metrics contexts ───────────────────────────────────────────

    [Fact]
    public void GetDatabaseMetrics_SkipsNullContext()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions();
        opts.AdditionalMetricsContexts.Add(null!);
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        var api     = new PengdowsCrudMonitoringApi(storage);
        // Should not throw; null context is skipped
        var metrics = api.GetDatabaseMetrics();
        Assert.NotEmpty(metrics);
    }

    [Fact]
    public void GetDatabaseMetrics_IncludesAdditionalContext()
    {
        var factory   = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx       = new DatabaseContext("Data Source=fake", factory);
        var extraCtx  = new DatabaseContext("Data Source=extra;", factory);
        var opts      = new PengdowsCrudStorageOptions();
        opts.AdditionalMetricsContexts.Add(extraCtx);
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        var api     = new PengdowsCrudMonitoringApi(storage);
        var metrics = api.GetDatabaseMetrics();
        Assert.NotEmpty(metrics);
    }

    [Fact]
    public void GetDatabaseMetricGrid_SkipsNullContext()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions();
        opts.AdditionalMetricsContexts.Add(null!);
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        var api     = new PengdowsCrudMonitoringApi(storage);
        var grid = api.GetDatabaseMetricGrid();
        Assert.NotEmpty(grid);
    }

    [Fact]
    public void GetDatabaseMetricGrid_IncludesAdditionalContext()
    {
        var factory  = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx      = new DatabaseContext("Data Source=fake", factory);
        var extraCtx = new DatabaseContext("Data Source=extra;", factory);
        var opts     = new PengdowsCrudStorageOptions();
        opts.AdditionalMetricsContexts.Add(extraCtx);
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        var api     = new PengdowsCrudMonitoringApi(storage);
        var grid = api.GetDatabaseMetricGrid();
        Assert.NotEmpty(grid);
    }

    // ── Servers with null Data ────────────────────────────────────────────────

    [Fact]
    public void Servers_WithNullData_ReturnsDefaultWorkerCount()
    {
        var (api, factory) = CreateApi();
        // Omit "Data" column → s.Data = null → if (s.Data != null) false branch
        factory.EnqueueReaderResult(new[] {
            new Dictionary<string, object> { ["Id"] = "srv1", ["LastHeartbeat"] = DateTime.UtcNow }
        });
        var result = api.Servers();
        Assert.Single(result);
        Assert.Equal(0, result[0].WorkersCount);
    }

    // ── TryParseDateTime branches ─────────────────────────────────────────────

    [Fact]
    public void ScheduledJobs_WithStateData_ParsesDates()
    {
        var (api, factory) = CreateApi();
        var now     = DateTime.UtcNow;
        var invData = MakeInvocationData();

        // LoadStateBatch/GetLatestAsync (2nd call) — state with date JSON
        var cState = new fakeDbConnection();
        cState.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"]        = (object?)200L,
            ["JobId"]     = 1L,
            ["Name"]      = "Scheduled",
            ["CreatedAt"] = now,
            ["Data"]      = "{\"EnqueueAt\":\"2025-01-01T00:00:00Z\",\"ScheduledAt\":\"2025-01-01T00:00:00Z\"}"
        }});
        factory.Connections.Insert(0, cState);

        // GetPagedByStateAsync (1st call)
        var cJob = new fakeDbConnection();
        cJob.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["StateId"] = 1L, ["StateName"] = "Scheduled",
            ["InvocationData"] = invData, ["Arguments"] = "[]",
            ["CreatedAt"] = now, ["ExpireAt"] = (object?)now.AddHours(1)
        }});
        factory.Connections.Insert(0, cJob);

        var result = api.ScheduledJobs(0, 10);
        Assert.Single(result);
        // EnqueueAt should be the parsed valid date, not MinValue
        Assert.NotEqual(DateTime.MinValue, result.First().Value.EnqueueAt);
    }

    [Fact]
    public void ProcessingJobs_WithInvalidDate_HandlesFallback()
    {
        var (api, factory) = CreateApi();
        var now     = DateTime.UtcNow;
        var invData = MakeInvocationData();

        // LoadStateBatch/GetLatestAsync (2nd call) — state with invalid date
        var cState = new fakeDbConnection();
        cState.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"]        = (object?)201L,
            ["JobId"]     = 1L,
            ["Name"]      = "Processing",
            ["CreatedAt"] = now,
            ["Data"]      = "{\"StartedAt\":\"not-a-valid-date\",\"ServerId\":\"worker-1\"}"
        }});
        factory.Connections.Insert(0, cState);

        // GetPagedByStateAsync (1st call)
        var cJob = new fakeDbConnection();
        cJob.EnqueueReaderResult(new[] { new Dictionary<string, object?> {
            ["Id"] = (object?)1L, ["StateId"] = 1L, ["StateName"] = "Processing",
            ["InvocationData"] = invData, ["Arguments"] = "[]",
            ["CreatedAt"] = now, ["ExpireAt"] = (object?)now.AddHours(1)
        }});
        factory.Connections.Insert(0, cJob);

        var result = api.ProcessingJobs(0, 10);
        Assert.Single(result);
        // StartedAt should be null since parse failed
        Assert.Null(result.First().Value.StartedAt);
    }

    // ── GetTimeline key-found branch ─────────────────────────────────────────

    [Fact]
    public void SucceededByDatesCount_WithMatchingKey_ReturnsValue()
    {
        var (api, factory) = CreateApi();
        // Use today's key so counts.TryGetValue returns true (the key-found branch)
        var todayKey = $"stats:succeeded:{DateTime.UtcNow.Date:yyyy-MM-dd}";
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Key"] = todayKey, ["Value"] = 42L
        }});
        var result = api.SucceededByDatesCount();
        Assert.Equal(7, result.Count);
        Assert.Equal(42L, result[DateTime.UtcNow.Date]);
    }

    // ── DeserializeJob catch branch ───────────────────────────────────────────

    [Fact]
    public void ProcessingJobs_WithUnresolvableType_SetsLoadException()
    {
        var (api, factory) = CreateApi();
        // InvocationData with a type that doesn't exist → DeserializeJob throws JobLoadException
        const string badInvData =
            "{\"Type\":\"No.Such.Type, NoSuchAssembly\",\"Method\":\"Run\"," +
            "\"ParameterTypes\":\"[]\",\"Arguments\":null}";
        factory.EnqueueReaderResult(new[] { new Dictionary<string, object> {
            ["Id"]             = 1L,
            ["StateId"]        = 1L,
            ["StateName"]      = "Processing",
            ["InvocationData"] = badInvData,
            ["Arguments"]      = "[]",
            ["CreatedAt"]      = DateTime.UtcNow,
            ["ExpireAt"]       = DateTime.UtcNow.AddHours(1)
        }});
        var result = api.ProcessingJobs(0, 10);
        Assert.Single(result);
        Assert.NotNull(result.First().Value.LoadException);
    }
}
