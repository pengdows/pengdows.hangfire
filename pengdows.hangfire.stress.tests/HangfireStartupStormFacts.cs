using System.Collections.Concurrent;
using System.Diagnostics;
using Hangfire;
using Hangfire.Logging;
using Hangfire.PostgreSql;
using Npgsql;
using pengdows.crud;
using pengdows.hangfire.stress.tests.infrastructure;
using Testcontainers.PostgreSql;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.stress.tests;

/// <summary>
/// Hangfire starts every background process and every worker at once, and by default a worker pool
/// is Environment.ProcessorCount * 5 (40 on 8 cores). Public reports describe the result: pool
/// timeouts ("all pooled connections were in use"), connection counts far above the worker count,
/// and servers that never recover (HangfireIO/Hangfire #395, #1031, #1795, #2065 and the forum
/// threads — NOT independently verified here). These tests start a real BackgroundJobServer with the
/// default worker count against a PostgreSQL server capped well below the connections that startup
/// can request, with every pool left at its default, and record what actually happens.
///
/// The stock Hangfire.PostgreSql arm is a baseline measurement and asserts nothing. The pengdows
/// arm states the required behavior: a default-configured context must start Hangfire, run every
/// job, and never be refused by the server.
/// </summary>
[Collection("HangfireStartupStorm")]
public class HangfireStartupStormFacts : IAsyncLifetime
{
    private const int ConfiguredMaxConnections = 200;
    private const int Workers = 40;      // Hangfire's default on an 8-core machine
    private const int Jobs = 200;

    private readonly ITestOutputHelper _out;
    private PostgreSqlContainer? _container;
    private string _adminConnectionString = string.Empty;
    private string _monitorConnectionString = string.Empty;
    private int _serverMaxConnections;

    public HangfireStartupStormFacts(ITestOutputHelper output) => _out = output;

    public async Task InitializeAsync()
    {
        try
        {
            _container = new PostgreSqlBuilder("postgres:16-alpine")
                .WithCommand("-c", $"max_connections={ConfiguredMaxConnections}")
                .Build();
            await _container.StartAsync();
        }
        catch (Exception ex)
        {
            _out.WriteLine($"Docker/PostgreSQL unavailable: {ex.Message}");
            _container = null;
            return;
        }

        _adminConnectionString = _container.GetConnectionString() + ";Pooling=false";
        _monitorConnectionString = _adminConnectionString;

        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await using var command = admin.CreateCommand();
        command.CommandText = "SHOW max_connections";
        _serverMaxConnections = Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    // Like the other stress facts, fail loudly when Docker is missing: a silent pass would read as proof.
    private void RequireContainer()
    {
        if (_container == null)
        {
            throw new InvalidOperationException("Docker/PostgreSQL container is not available.");
        }
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }
    }

    [Fact(Timeout = 300_000)]
    public async Task StockHangfirePostgreSql_DefaultWorkers_Baseline()
    {
        RequireContainer();

        var connStr = await CreateDatabaseAsync("stock");
        var storage = new PostgreSqlStorage(connStr, new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = true });

        var result = await RunStartupStormAsync(storage, "stock");
        Report("stock Hangfire.PostgreSql", result);
        // Baseline: records behaviour, asserts nothing.
    }

    [Fact(Timeout = 300_000)]
    public async Task PengdowsHangfire_DefaultConfiguration_StartsAndRunsEveryJob_WithoutBeingRefusedByTheServer()
    {
        RequireContainer();

        var connStr = await CreateDatabaseAsync("pengdows");
        using var context = new DatabaseContext(connStr, NpgsqlFactory.Instance);
        await SchemaHelper.InstallFromResourceAsync(context, "PostgresInstall.sql");

        var storage = new PengdowsCrudJobStorage(context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema = false,
            DistributedLockTtl = TimeSpan.FromSeconds(30),
        });
        storage.Initialize();

        var result = await RunStartupStormAsync(storage, "pengdows");
        Report("pengdows.hangfire (default DatabaseContext)", result);

        Assert.Equal(0, result.ConnectionLimitErrors);
        Assert.Equal(Jobs, result.Completed);
    }

    // ── harness ────────────────────────────────────────────────────────────────

    private async Task<string> CreateDatabaseAsync(string name)
    {
        await using var admin = new NpgsqlConnection(_adminConnectionString);
        await admin.OpenAsync();
        await using var cmd = admin.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE {name}";
        await cmd.ExecuteNonQueryAsync();

        var builder = new NpgsqlConnectionStringBuilder(_container!.GetConnectionString()) { Database = name };
        return builder.ConnectionString; // pool settings deliberately left at the provider default
    }

    private async Task<StormResult> RunStartupStormAsync(JobStorage storage, string label)
    {
        var logs = new CountingLogProvider();
        LogProvider.SetCurrentLogProvider(logs);
        StormJob.Reset();

        var peak = 0;
        using var sampling = new CancellationTokenSource();
        var sampler = Task.Run(async () =>
        {
            await using var monitor = new NpgsqlConnection(_monitorConnectionString);
            await monitor.OpenAsync();
            while (!sampling.IsCancellationRequested)
            {
                try
                {
                    await using var cmd = monitor.CreateCommand();
                    cmd.CommandText =
                        "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()";
                    var n = Convert.ToInt32(await cmd.ExecuteScalarAsync());
                    InterlockedMax(ref peak, n);
                }
                catch
                {
                    // the monitor itself can be refused while the server is full; keep sampling
                }

                await Task.Delay(25);
            }
        });

        var client = new BackgroundJobClient(storage);
        var enqueueFailures = 0;
        for (var i = 0; i < Jobs; i++)
        {
            try
            {
                client.Enqueue(() => StormJob.Run());
            }
            catch
            {
                enqueueFailures++;
            }
        }

        var sw = Stopwatch.StartNew();
        using (var server = new BackgroundJobServer(
                   new BackgroundJobServerOptions { WorkerCount = Workers, ServerName = $"storm-{label}" }, storage))
        {
            while (StormJob.Completed < Jobs - enqueueFailures && sw.Elapsed < TimeSpan.FromSeconds(90))
            {
                await Task.Delay(250);
            }
        }

        sw.Stop();
        sampling.Cancel();
        await sampler;

        return new StormResult(
            StormJob.Completed, enqueueFailures, peak, sw.Elapsed,
            logs.CountMatching("too many clients", "53300", "TooManyConnections", "remaining connection slots"),
            logs.CountMatching("pool", "Timeout", "timed out"),
            logs.ErrorCount,
            logs.Samples(),
            logs.AllMessages);
    }

    private void Report(string label, StormResult r)
    {
        _out.WriteLine(
            $"{label}: server max_connections={_serverMaxConnections}, workers={Workers}, jobs={Jobs}; " +
            $"completed={r.Completed}, enqueue failures={r.EnqueueFailures}, peak server connections={r.PeakConnections}, " +
            $"elapsed={r.Elapsed.TotalSeconds:F1}s; log messages seen={r.AllLogMessages}, warn/error={r.LogErrors} " +
            $"(connection-limit={r.ConnectionLimitErrors}, pool/timeout={r.PoolTimeoutErrors})");
        foreach (var s in r.Samples)
        {
            _out.WriteLine("  " + s);
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value &&
               Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    private sealed record StormResult(
        int Completed, int EnqueueFailures, int PeakConnections, TimeSpan Elapsed,
        int ConnectionLimitErrors, int PoolTimeoutErrors, int LogErrors, IReadOnlyList<string> Samples, int AllLogMessages);

    private sealed class CountingLogProvider : ILogProvider
    {
        private readonly ConcurrentQueue<string> _errors = new();
        private int _allMessages;
        public int ErrorCount => _errors.Count;
        public int AllMessages => Volatile.Read(ref _allMessages);

        public int CountMatching(params string[] needles) =>
            _errors.Count(e => needles.Any(n => e.Contains(n, StringComparison.OrdinalIgnoreCase)));

        public IReadOnlyList<string> Samples() =>
            _errors.GroupBy(e => e.Length > 140 ? e[..140] : e).OrderByDescending(g => g.Count()).Take(4)
                .Select(g => $"{g.Count()}x {g.Key}").ToList();

        public ILog GetLogger(string name) => new Logger(_errors, () => Interlocked.Increment(ref _allMessages));
        public IDisposable OpenNestedContext(string message) => NoopDisposable.Instance;
        public IDisposable OpenMappedContext(string key, object value, bool destructure = false) => NoopDisposable.Instance;

        private sealed class Logger(ConcurrentQueue<string> errors, Func<int> countAll) : ILog
        {
            public bool Log(Hangfire.Logging.LogLevel logLevel, Func<string>? messageFunc, Exception? exception = null)
            {
                if (messageFunc == null)
                {
                    return true; // "is this level enabled" probe: enable everything so nothing is hidden
                }

                countAll();

                if (logLevel >= Hangfire.Logging.LogLevel.Warn)
                {
                    errors.Enqueue($"{messageFunc()} {exception?.GetType().Name}: {exception?.Message}");
                }

                return true;
            }
        }

        private sealed class NoopDisposable : IDisposable
        {
            public static readonly NoopDisposable Instance = new();
            public void Dispose() { }
        }
    }
}

public static class StormJob
{
    // Real jobs run for seconds, not milliseconds: long enough that every worker is busy at once.
    public const int JobMilliseconds = 1500;

    private static int _completed;
    public static int Completed => Volatile.Read(ref _completed);
    public static void Reset() => Interlocked.Exchange(ref _completed, 0);

    public static void Run()
    {
        Thread.Sleep(JobMilliseconds);
        Interlocked.Increment(ref _completed);
    }
}
