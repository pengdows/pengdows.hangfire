using System;
using System.Data;
using System.Threading.Tasks;
using Hangfire.SqlServer;
using Microsoft.Data.SqlClient;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.metrics;
using Testcontainers.MsSql;
using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

/// <summary>
/// xUnit class fixture that spins up SQL Server in a Docker container,
/// installs the Hangfire schema via the production installer, and exposes
/// a ready-to-use <see cref="PengdowsCrudJobStorage"/> for all stress tests
/// in the collection.
///
/// The container starts once per test collection run — all tests share the
/// same schema. Each test uses unique resource names (Guid-based) so tests
/// do not interfere with each other.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private MsSqlContainer _container    = null!;
    private string         _baseConnStr  = null!;

    public PengdowsCrudJobStorage Storage          { get; private set; } = null!;
    public IDatabaseContext        Context          { get; private set; } = null!;

    /// <summary>
    /// Hangfire's stock <c>SqlServerStorage</c> connected to the same container.
    /// Used by baseline comparison tests to run the exact same stress scenarios
    /// against Hangfire's built-in <c>sp_getapplock</c> distributed lock.
    /// Schema is installed under <c>[HangFireBaseline]</c> to avoid conflicts.
    /// </summary>
    public SqlServerStorage BaselineStorage { get; private set; } = null!;

    /// <summary>
    /// Raw SQL Server connection string (no pool-size suffix).
    /// Use this when passing the connection string to an out-of-process worker.
    /// </summary>
    public string BaseConnectionString => _baseConnStr;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();

        await _container.StartAsync();

        _baseConnStr = _container.GetConnectionString();

        // Pool size 256 so correctness tests (up to 200 workers) are never
        // throttled by the pool governor — pool pressure is intentionally
        // removed so that lock-logic failures are the only failure mode.
        Context = BuildContext(256);

        Storage = new PengdowsCrudJobStorage(Context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema  = true,
            DistributedLockTtl = TimeSpan.FromSeconds(30),
        });
        Storage.Initialize();

        BaselineStorage = new SqlServerStorage(
            _baseConnStr,
            new SqlServerStorageOptions
            {
                PrepareSchemaIfNecessary = true,
                SchemaName               = "HangFireBaseline", // separate schema — no conflict with our tables
            });
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    /// <summary>
    /// Creates a storage instance backed by a fresh <see cref="IDatabaseContext"/>
    /// whose connection pool is constrained to <paramref name="poolSize"/>.
    ///
    /// Use this for boundary-pressure tests where pool size is intentionally
    /// set below worker count.  The shared <see cref="Context"/> is unaffected.
    /// </summary>
    public PengdowsCrudJobStorage CreateStorageWithPoolSize(
        int poolSize, TimeSpan? ttl = null) =>
        new PengdowsCrudJobStorage(
            BuildContext(poolSize),
            new PengdowsCrudStorageOptions
            {
                AutoPrepareSchema  = false,
                DistributedLockTtl = ttl ?? TimeSpan.FromSeconds(30),
            });

    /// <summary>
    /// Creates a storage instance that shares the same database context but
    /// uses a different TTL. Used by tests that need to observe TTL expiry
    /// quickly without waiting 30 seconds.
    /// </summary>
    public PengdowsCrudJobStorage CreateStorageWithTtl(TimeSpan ttl) =>
        new PengdowsCrudJobStorage(Context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema  = false,
            DistributedLockTtl = ttl,
        });


    private IDatabaseContext BuildContext(int poolSize) =>
        new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString    = _baseConnStr + $";Max Pool Size={poolSize};",
                MaxConcurrentWrites = poolSize,
                MaxConcurrentReads  = poolSize,
                EnableMetrics       = true,
                MetricsOptions      = new MetricsOptions { EnableApproxPercentiles = true }
            },
            SqlClientFactory.Instance);

    /// <summary>Executes a scalar query against the shared context.</summary>
    public async Task<T?> QueryScalarAsync<T>(string sql, params (string name, object? value)[] parameters)
    {
        await using var sc = Context.CreateSqlContainer(sql);
        foreach (var (name, value) in parameters)
        {
            var dbType = value switch
            {
                string   => DbType.String,
                long     => DbType.Int64,
                int      => DbType.Int32,
                DateTime => DbType.DateTime,
                _        => DbType.String,
            };
            sc.AddParameterWithValue(name, dbType, value);
        }
        return await sc.ExecuteScalarOrNullAsync<T>();
    }
}
