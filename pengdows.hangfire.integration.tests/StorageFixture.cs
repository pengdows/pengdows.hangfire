using System.Data.Common;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using FirebirdSql.Data.FirebirdClient;
using DuckDB.NET.Data;
using pengdows.crud;
using Xunit;
using pengdows.hangfire.models;
using System.Data;

namespace pengdows.hangfire.integration.tests;

public abstract class StorageFixture : IAsyncLifetime
{
    public PengdowsCrudJobStorage Storage { get; protected set; } = null!;
    public DatabaseContext Context { get; protected set; } = null!;

    protected abstract string ConnectionString { get; }
    protected abstract DbProviderFactory Factory { get; }

    public virtual async Task InitializeAsync()
    {
        Context = new DatabaseContext(
            new pengdows.crud.configuration.DatabaseContextConfiguration
            {
                ConnectionString = ConnectionString,
                EnableMetrics = true,
                MetricsOptions = new pengdows.crud.metrics.MetricsOptions { EnableApproxPercentiles = true }
            },
            Factory);
        
        // Setup schema using Liquibase or manual DDL
        await InstallSchemaAsync();

        Storage = new PengdowsCrudJobStorage(Context, 
            new PengdowsCrudStorageOptions { AutoPrepareSchema = false });
    }

    protected abstract Task InstallSchemaAsync();

    public virtual async Task DisposeAsync()
    {
        if (Storage != null)
        {
            var monitor = Storage.GetMonitoringApi() as PengdowsCrudMonitoringApi;
            if (monitor != null)
            {
                // We use Console.WriteLine so it shows up in standard output/test results
                // even when captured by xUnit.
                Console.WriteLine(monitor.GetDatabaseMetricGrid());
            }
        }

        Context?.Dispose();
    }

    // ── Portable Arrangement Helpers ──────────────────────────────────────────
    // Use the gateways directly so pengdows.crud handles dialect/schema/identity logic.
    
    public async Task<long> InsertJobAsync(
        string invocationData = "{}",
        string arguments = "[]",
        string? stateName = null,
        DateTime? expireAt = null)
    {
        var job = new Job
        {
            InvocationData = invocationData,
            Arguments = arguments,
            StateName = stateName,
            CreatedAt = DateTime.UtcNow,
            ExpireAt = expireAt
        };
        await Storage.Jobs.CreateAsync(job);
        return job.ID;
    }

    public async Task<long> InsertJobQueueAsync(long jobId, string queue, DateTime? fetchedAt = null)
    {
        var jq = new JobQueue
        {
            JobID = jobId,
            Queue = queue,
            FetchedAt = fetchedAt
        };
        await Storage.JobQueues.CreateAsync(jq);
        return jq.ID;
    }

    public async Task InsertHashAsync(string key, string field, string? value = null, DateTime? expireAt = null)
    {
        var h = new Hash { Key = key, Field = field, Value = value, ExpireAt = expireAt };
        await Storage.Hashes.UpsertAsync(h);
    }

    public async Task InsertSetAsync(string key, string value, double score = 0.0, DateTime? expireAt = null)
    {
        var s = new Set { Key = key, Value = value, Score = score, ExpireAt = expireAt };
        await Storage.Sets.UpsertAsync(s);
    }

    public async Task InsertListAsync(string key, string? value = null, DateTime? expireAt = null)
    {
        var l = new List { Key = key, Value = value, ExpireAt = expireAt };
        await Storage.Lists.CreateAsync(l);
    }

    public async Task InsertCounterAsync(string key, long value = 1, DateTime? expireAt = null)
    {
        await Storage.Counters.AppendAsync(key, (int)value, expireAt);
    }

    // ── Verification Helper ───────────────────────────────────────────────────

    public async Task<T?> QueryScalarAsync<T>(string sql, params (string name, object? value)[] parameters)
    {
        await using var sc = Context.CreateSqlContainer(sql);
        foreach (var (name, value) in parameters)
        {
            var dbType = value switch
            {
                string => DbType.String,
                long => DbType.Int64,
                int => DbType.Int32,
                DateTime => DbType.DateTime,
                bool => DbType.Boolean,
                _ => DbType.String
            };
            sc.AddParameterWithValue(name, dbType, value);
        }
        return await sc.ExecuteScalarOrNullAsync<T>();
    }

    protected virtual async Task<long> GetLastIdAsync()
    {
        string sql = Context.Product switch
        {
            pengdows.crud.enums.SupportedDatabase.SqlServer => "SELECT SCOPE_IDENTITY()",
            pengdows.crud.enums.SupportedDatabase.PostgreSql => "SELECT lastval()",
            pengdows.crud.enums.SupportedDatabase.MySql => "SELECT LAST_INSERT_ID()",
            pengdows.crud.enums.SupportedDatabase.Sqlite => "SELECT last_insert_rowid()",
            _ => throw new NotSupportedException($"GetLastId not implemented for {Context.Product}")
        };

        await using var sc = Context.CreateSqlContainer(sql);
        return await sc.ExecuteScalarRequiredAsync<long>();
    }
}
