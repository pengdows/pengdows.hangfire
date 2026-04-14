using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.metrics;
using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

public abstract class BaseStressFixture : IAsyncLifetime
{
    public PengdowsCrudJobStorage Storage { get; protected set; } = null!;
    public IDatabaseContext Context { get; protected set; } = null!;

    protected abstract string ConnectionString { get; }
    protected abstract DbProviderFactory Factory { get; }
    protected virtual int DefaultPoolSize => 256;
    protected virtual string PoolSizeKeyword => "Max Pool Size";

    public virtual async Task InitializeAsync()
    {
        await StartContainerAsync();

        Context = BuildContext(DefaultPoolSize);

        await InstallSchemaAsync();

        Storage = new PengdowsCrudJobStorage(Context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema = false,
            DistributedLockTtl = TimeSpan.FromSeconds(30),
        });
        Storage.Initialize();
    }

    public virtual async Task DisposeAsync()
    {
        Context?.Dispose();
        await StopContainerAsync();
    }

    protected abstract Task StartContainerAsync();
    protected abstract Task StopContainerAsync();
    protected abstract Task InstallSchemaAsync();

    public IDatabaseContext BuildContext(int poolSize) =>
        new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = ConnectionString + (ConnectionString.Contains("Pool Size") ? "" : $";{PoolSizeKeyword}={poolSize};"),
                MaxConcurrentWrites = poolSize,
                MaxConcurrentReads = poolSize,
                EnableMetrics = true,
                MetricsOptions = new MetricsOptions { EnableApproxPercentiles = true }
            },
            Factory);

    public PengdowsCrudJobStorage CreateStorageWithPoolSize(int poolSize, TimeSpan? ttl = null) =>
        new PengdowsCrudJobStorage(
            BuildContext(poolSize),
            new PengdowsCrudStorageOptions
            {
                AutoPrepareSchema = false,
                DistributedLockTtl = ttl ?? TimeSpan.FromSeconds(30),
            });

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
                _ => DbType.String,
            };
            sc.AddParameterWithValue(name, dbType, value);
        }
        return await sc.ExecuteScalarOrNullAsync<T>();
    }
}

internal static class SchemaHelper
{
    public static async Task InstallFromResourceAsync(IDatabaseContext db, string resourceName)
    {
        // AppContext.BaseDirectory = bin/Debug/net8.0 — go up 4 levels to reach the solution root.
        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        var ddlPath = Path.Combine(root, "pengdows.hangfire.integration.tests", resourceName);
        var ddl = await File.ReadAllTextAsync(ddlPath);

        foreach (var stmt in ddl.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            // Strip line comments before deciding whether the chunk is empty.
            var stripped = string.Join('\n',
                stmt.Split('\n').Where(line => !line.TrimStart().StartsWith("--"))).Trim();
            if (string.IsNullOrWhiteSpace(stripped)) continue;
            await using var sc = db.CreateSqlContainer(stripped);
            await sc.ExecuteNonQueryAsync();
        }
    }
}
