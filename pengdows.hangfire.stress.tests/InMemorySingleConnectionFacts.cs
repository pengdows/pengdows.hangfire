using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.enums;
using Xunit;

namespace pengdows.hangfire.stress.tests;

public sealed class InMemorySingleConnectionFacts
{
    [Fact(Timeout = 30_000)]
    public async Task SqliteMemory_ConcurrentFetchesDoNotDeadlock()
    {
        await using var context = new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = "Data Source=:memory:",
                DbMode = DbMode.SingleConnection,
                ModeLockTimeout = TimeSpan.FromSeconds(5)
            },
            SqliteFactory.Instance);

        await ExecuteAsync(context, "ATTACH DATABASE ':memory:' AS \"HangFire\"");
        await ExecuteAsync(context, "CREATE TABLE \"HangFire\".\"JobQueue\" (\"Id\" INTEGER PRIMARY KEY, \"Queue\" TEXT NOT NULL, \"JobId\" INTEGER NOT NULL, \"FetchedAt\" TEXT NULL, \"FetchToken\" TEXT NULL)");
        await ExecuteAsync(context, "INSERT INTO \"HangFire\".\"JobQueue\" (\"Id\", \"Queue\", \"JobId\") VALUES (1, 'default', 42)");

        var storage = new PengdowsCrudJobStorage(context, new PengdowsCrudStorageOptions { AutoPrepareSchema = false });
        storage.Initialize();

        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ =>
            storage.JobQueues.FetchNextJobAsync(["default"], CancellationToken.None)));

        Assert.Single(results, result => result is not null);
    }

    [Fact(Timeout = 30_000)]
    public async Task DuckDbMemory_ConcurrentFetchesDoNotDeadlock()
    {
        await using var context = new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = "DataSource=:memory:",
                DbMode = DbMode.SingleConnection,
                ModeLockTimeout = TimeSpan.FromSeconds(5)
            },
            DuckDBClientFactory.Instance);

        await ExecuteAsync(context, "CREATE SCHEMA \"HangFire\"");
        await ExecuteAsync(context, "CREATE TABLE \"HangFire\".\"JobQueue\" (\"Id\" BIGINT PRIMARY KEY, \"Queue\" TEXT NOT NULL, \"JobId\" BIGINT NOT NULL, \"FetchedAt\" TIMESTAMP NULL, \"FetchToken\" VARCHAR(64) NULL)");
        await ExecuteAsync(context, "INSERT INTO \"HangFire\".\"JobQueue\" (\"Id\", \"Queue\", \"JobId\") VALUES (1, 'default', 42)");

        var storage = new PengdowsCrudJobStorage(context, new PengdowsCrudStorageOptions { AutoPrepareSchema = false });
        storage.Initialize();

        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ =>
            storage.JobQueues.FetchNextJobAsync(["default"], CancellationToken.None)));

        Assert.Single(results, result => result is not null);
    }

    private static async Task ExecuteAsync(IDatabaseContext context, string sql)
    {
        await using var container = context.CreateSqlContainer(sql);
        await container.ExecuteNonQueryAsync();
    }
}
