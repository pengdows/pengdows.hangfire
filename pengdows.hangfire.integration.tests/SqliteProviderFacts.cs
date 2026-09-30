using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using pengdows.hangfire;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

/// <summary>
/// SQLite-specific integration coverage. The shared fixture tests exercise the
/// common gateway contract, while these tests protect behavior that differs in
/// SQLite: no schema namespace, SQLite-generated row ids, TEXT-backed values,
/// and INSERT ... ON CONFLICT upserts.
/// </summary>
[Collection("Sqlite")]
public sealed class SqliteProviderFacts
{
    private readonly SqliteFixture _f;

    public SqliteProviderFacts(SqliteFixture fixture) => _f = fixture;

    [Fact]
    public async Task BackgroundJobServer_LongRunningFetchedJob_IsNotExecutedTwice()
    {
        var key = Guid.NewGuid().ToString("N");
        var state = new BlockingJobState();
        LongRunningIntegrationJob.States[key] = state;
        var options = _f.Storage.Options;
        options.InvisibilityTimeout = TimeSpan.FromSeconds(2);
        options.QueuePollInterval = TimeSpan.FromMilliseconds(100);
        options.QueuePollJitter = false;

        try
        {
            var client = new BackgroundJobClient(_f.Storage);
            var jobId = client.Enqueue(() => LongRunningIntegrationJob.Run(key));
            var jobData = new PengdowsCrudConnection(_f.Storage).GetJobData(jobId);
            Assert.NotNull(jobData);
            Assert.NotNull(jobData!.InvocationData.DeserializeJob());
            using var server = new BackgroundJobServer(new BackgroundJobServerOptions
            {
                WorkerCount = 1,
                Queues = ["default"],
                ShutdownTimeout = TimeSpan.FromSeconds(10)
            }, _f.Storage);

            if (!state.Started.Wait(TimeSpan.FromSeconds(10)))
            {
                Assert.Fail("Job did not start.");
            }
            await Task.Delay(TimeSpan.FromSeconds(4));
            state.Release.Set();
            Assert.True(state.Completed.Wait(TimeSpan.FromSeconds(10)), "Job did not complete.");
            Assert.Equal(1, state.Invocations);
        }
        finally
        {
            LongRunningIntegrationJob.States.TryRemove(key, out _);
        }
    }

    internal sealed class BlockingJobState
    {
        public int Invocations;
        public ManualResetEventSlim Started { get; } = new();
        public ManualResetEventSlim Release { get; } = new();
        public ManualResetEventSlim Completed { get; } = new();
    }

    [Fact]
    public async Task Schema_UsesBareTables_AndStoresRowsInMainDatabase()
    {
        var jobId = await _f.InsertJobAsync();

        var tableName = await _f.QueryScalarAsync<string>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'Job'");
        var namespacedTable = await _f.QueryScalarAsync<string>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'HangFire.Job'");
        var storedId = await _f.QueryScalarAsync<long>(
            "SELECT Id FROM Job WHERE Id = @id", ("@id", jobId));

        Assert.Equal("Job", tableName);
        Assert.Null(namespacedTable);
        Assert.Equal(jobId, storedId);
    }

    [Fact]
    public async Task Job_IdentityAndNullableTextFields_RoundTrip()
    {
        var expireAt = DateTime.UtcNow.AddMinutes(17);
        var jobId = await _f.InsertJobAsync(
            invocationData: "{\"type\":\"sqlite\"}",
            arguments: "[null,\"quoted \\\"value\\\"\"]",
            stateName: "Succeeded",
            expireAt: expireAt);

        var job = await _f.Storage.Jobs.RetrieveOneAsync(jobId);

        Assert.NotNull(job);
        Assert.True(jobId > 0);
        Assert.Equal("{\"type\":\"sqlite\"}", job.InvocationData);
        Assert.Equal("[null,\"quoted \\\"value\\\"\"]", job.Arguments);
        Assert.Equal("Succeeded", job.StateName);
        Assert.NotNull(job.ExpireAt);
        Assert.Equal(expireAt, job.ExpireAt.Value);

        var noExpiryId = await _f.InsertJobAsync();
        var noExpiryJob = await _f.Storage.Jobs.RetrieveOneAsync(noExpiryId);
        Assert.NotNull(noExpiryJob);
        Assert.Null(noExpiryJob.ExpireAt);
        Assert.Null(noExpiryJob.StateName);
    }

    [Fact]
    public async Task HashUpsert_UpdatesExistingCompositeKeyWithoutDuplicate()
    {
        var key = "sqlite-upsert-" + Guid.NewGuid();

        await _f.InsertHashAsync(key, "field", "first");
        await _f.InsertHashAsync(key, "field", "second");

        var rows = await _f.Storage.Hashes.GetWhereAsync("Key", key);

        var row = Assert.Single(rows);
        Assert.Equal("field", row.Field);
        Assert.Equal("second", row.Value);
    }

    [Fact]
    public async Task SetUpsert_UpdatesScoreWithoutDuplicate()
    {
        var key = "sqlite-set-upsert-" + Guid.NewGuid();

        await _f.InsertSetAsync(key, "value", 1.25);
        await _f.InsertSetAsync(key, "value", 9.75);

        var rows = await _f.Storage.Sets.GetWhereAsync("Key", key);

        var row = Assert.Single(rows);
        Assert.Equal("value", row.Value);
        Assert.Equal(9.75, row.Score, 5);
    }
}

public static class LongRunningIntegrationJob
{
    internal static readonly ConcurrentDictionary<string, SqliteProviderFacts.BlockingJobState> States =
        new();

    public static void Run(string key)
    {
        var state = States[key];
        Interlocked.Increment(ref state.Invocations);
        state.Started.Set();
        state.Release.Wait(TimeSpan.FromSeconds(15));
        state.Completed.Set();
    }
}
