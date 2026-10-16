using System.Diagnostics;
using Hangfire;
using Npgsql;
using pengdows.crud;
using pengdows.hangfire.stress.tests.infrastructure;
using Testcontainers.PostgreSql;
using Xunit;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Exercises the real Hangfire startup path with more workers than the database
/// allows connections. The test requires progress and completion; a server that
/// starts but silently refuses jobs must fail.
/// </summary>
[Collection("HangfireStartupStorm")]
public sealed class HangfireStartupStormFacts : IAsyncLifetime
{
    private const int Jobs = 100;
    private const int Workers = 40;
    private PostgreSqlContainer _container = null!;

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithCommand("-c", "max_connections=30")
            .Build();
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
    }

    [Fact(Timeout = 300_000)]
    public async Task DefaultWorkerStartup_CompletesEveryQueuedJob()
    {
        var adminConnectionString = _container.GetConnectionString() + ";Pooling=false";
        var databaseName = "storm";

        await using (var admin = new NpgsqlConnection(adminConnectionString))
        {
            await admin.OpenAsync();
            await using var command = admin.CreateCommand();
            command.CommandText = "CREATE DATABASE \"storm\"";
            await command.ExecuteNonQueryAsync();
        }

        var connectionString = new NpgsqlConnectionStringBuilder(_container.GetConnectionString())
        {
            Database = databaseName,
            MaxPoolSize = 100
        }.ConnectionString;

        await using var context = new DatabaseContext(connectionString, NpgsqlFactory.Instance);
        await SchemaHelper.InstallFromResourceAsync(context, "PostgresInstall.sql");

        var storage = new PengdowsCrudJobStorage(context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema = false,
            DistributedLockTtl = TimeSpan.FromSeconds(30)
        });
        storage.Initialize();

        StartupStormJob.Reset();
        var client = new BackgroundJobClient(storage);
        for (var i = 0; i < Jobs; i++)
        {
            client.Enqueue(() => StartupStormJob.Run());
        }

        var stopwatch = Stopwatch.StartNew();
        using (var server = new BackgroundJobServer(
                   new BackgroundJobServerOptions
                   {
                       WorkerCount = Workers,
                       ServerName = "pengdows-startup-storm"
                   },
                   storage))
        {
            while (StartupStormJob.Completed < Jobs && stopwatch.Elapsed < TimeSpan.FromSeconds(90))
            {
                await Task.Delay(250);
            }
        }

        Assert.Equal(Jobs, StartupStormJob.Completed);
    }
}

public static class StartupStormJob
{
    private static int _completed;

    public static int Completed => Volatile.Read(ref _completed);

    public static void Reset() => Interlocked.Exchange(ref _completed, 0);

    public static void Run()
    {
        Thread.Sleep(500);
        Interlocked.Increment(ref _completed);
    }
}
