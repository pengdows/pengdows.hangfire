using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.metrics;
using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

public sealed class DuckDbStressFixture : IAsyncLifetime
{
    private string _dbFile = null!;
    public PengdowsCrudJobStorage Storage { get; private set; } = null!;
    public DatabaseContext Context { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _dbFile = $"stress_{Guid.NewGuid():N}.duckdb";
        
        Context = new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = $"DataSource={_dbFile}",
                EnableMetrics = true,
                MetricsOptions = new MetricsOptions { EnableApproxPercentiles = true }
            },
            DuckDBClientFactory.Instance);

        Storage = new PengdowsCrudJobStorage(Context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema = false,
            DistributedLockTtl = TimeSpan.FromSeconds(30),
        });
        
        // DuckDB needs explicit schema creation
        await using (var schemaSc = Context.CreateSqlContainer("CREATE SCHEMA IF NOT EXISTS \"HangFire\""))
        {
            await schemaSc.ExecuteNonQueryAsync();
        }

        // Manual install using embedded script
        var ddl = DuckDbInstallScript;
        foreach (var stmt in ddl.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.IsNullOrWhiteSpace(stmt) || stmt.StartsWith("--")) continue;
            
            // For DuckDB, we'll prefix the tables with the schema name if they don't have it
            var ddlStmt = stmt;
            if (ddlStmt.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase) && !ddlStmt.Contains("\"HangFire\""))
            {
                ddlStmt = ddlStmt.Replace("CREATE TABLE IF NOT EXISTS \"", "CREATE TABLE IF NOT EXISTS \"HangFire\".\"", StringComparison.OrdinalIgnoreCase);
            }
            else if (ddlStmt.StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && !ddlStmt.Contains("\"HangFire\""))
            {
                ddlStmt = ddlStmt.Replace("INTO \"", "INTO \"HangFire\".\"", StringComparison.OrdinalIgnoreCase);
            }

            await using var sc = Context.CreateSqlContainer(ddlStmt);
            await sc.ExecuteNonQueryAsync();
        }

        Storage.Initialize();
    }

    public async Task DisposeAsync()
    {
        Context?.Dispose();
        if (File.Exists(_dbFile))
        {
            try { File.Delete(_dbFile); } catch { }
        }
    }

    private static string DuckDbInstallScript = @"
CREATE SEQUENCE IF NOT EXISTS job_id_seq;
CREATE SEQUENCE IF NOT EXISTS state_id_seq;
CREATE SEQUENCE IF NOT EXISTS jobqueue_id_seq;
CREATE SEQUENCE IF NOT EXISTS list_id_seq;
CREATE SEQUENCE IF NOT EXISTS counter_id_seq;

CREATE TABLE IF NOT EXISTS ""Schema"" (
    ""Version"" INTEGER NOT NULL,
    PRIMARY KEY (""Version"")
);

CREATE TABLE IF NOT EXISTS ""Job"" (
    ""Id"" BIGINT PRIMARY KEY DEFAULT nextval('job_id_seq'),
    ""StateId"" BIGINT NULL,
    ""StateName"" TEXT NULL,
    ""InvocationData"" TEXT NOT NULL,
    ""Arguments"" TEXT NOT NULL,
    ""CreatedAt"" TIMESTAMP NOT NULL,
    ""ExpireAt"" TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS ""State"" (
    ""Id"" BIGINT PRIMARY KEY DEFAULT nextval('state_id_seq'),
    ""JobId"" BIGINT NOT NULL,
    ""Name"" TEXT NOT NULL,
    ""Reason"" TEXT NULL,
    ""CreatedAt"" TIMESTAMP NOT NULL,
    ""Data"" TEXT NULL
);

CREATE TABLE IF NOT EXISTS ""JobParameter"" (
    ""JobId"" BIGINT NOT NULL,
    ""Name"" TEXT NOT NULL,
    ""Value"" TEXT NULL,
    PRIMARY KEY (""JobId"", ""Name"")
);

CREATE TABLE IF NOT EXISTS ""JobQueue"" (
    ""Id"" BIGINT PRIMARY KEY DEFAULT nextval('jobqueue_id_seq'),
    ""Queue"" TEXT NOT NULL,
    ""JobId"" BIGINT NOT NULL,
    ""FetchedAt"" TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS ""Server"" (
    ""Id"" TEXT NOT NULL,
    ""Data"" TEXT NULL,
    ""LastHeartbeat"" TIMESTAMP NOT NULL,
    PRIMARY KEY (""Id"")
);

CREATE TABLE IF NOT EXISTS ""Hash"" (
    ""Key"" TEXT NOT NULL,
    ""Field"" TEXT NOT NULL,
    ""Value"" TEXT NULL,
    ""ExpireAt"" TIMESTAMP NULL,
    PRIMARY KEY (""Key"", ""Field"")
);

CREATE TABLE IF NOT EXISTS ""List"" (
    ""Key"" TEXT NOT NULL,
    ""Id"" BIGINT PRIMARY KEY DEFAULT nextval('list_id_seq'),
    ""Value"" TEXT NULL,
    ""ExpireAt"" TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS ""Set"" (
    ""Key"" TEXT NOT NULL,
    ""Value"" TEXT NOT NULL,
    ""Score"" DOUBLE NOT NULL DEFAULT 0.0,
    ""ExpireAt"" TIMESTAMP NULL,
    PRIMARY KEY (""Key"", ""Value"")
);

CREATE TABLE IF NOT EXISTS ""Counter"" (
    ""Key"" TEXT NOT NULL,
    ""Id"" BIGINT PRIMARY KEY DEFAULT nextval('counter_id_seq'),
    ""Value"" INTEGER NOT NULL,
    ""ExpireAt"" TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS ""AggregatedCounter"" (
    ""Key"" TEXT NOT NULL,
    ""Value"" BIGINT NOT NULL DEFAULT 0,
    ""ExpireAt"" TIMESTAMP NULL,
    PRIMARY KEY (""Key"")
);

CREATE TABLE IF NOT EXISTS ""hf_lock"" (
    ""resource""   TEXT    NOT NULL,
    ""owner_id""   TEXT    NOT NULL,
    ""expires_at"" TIMESTAMP NOT NULL,
    ""version""    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (""resource"")
);

INSERT OR IGNORE INTO ""Schema"" (""Version"") VALUES (9);
";
}

[CollectionDefinition("DuckDbStress")]
public class DuckDbStressCollection : ICollectionFixture<DuckDbStressFixture> { }
