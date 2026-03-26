using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.metrics;
using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

public sealed class SqliteStressFixture : IAsyncLifetime
{
    private string _dbFile = null!;
    public PengdowsCrudJobStorage Storage { get; private set; } = null!;
    public DatabaseContext Context { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _dbFile = $"stress_{Guid.NewGuid():N}.db";
        
        // SingleWriter mode is auto-selected for file-based SQLite
        Context = new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = $"Data Source={_dbFile}",
                EnableMetrics = true,
                MetricsOptions = new MetricsOptions { EnableApproxPercentiles = true }
            },
            SqliteFactory.Instance);

        Storage = new PengdowsCrudJobStorage(Context, new PengdowsCrudStorageOptions
        {
            AutoPrepareSchema = false,
            DistributedLockTtl = TimeSpan.FromSeconds(30),
        });
        
        // Ensure "HangFire" schema is attached for the installer queries
        await using (var attachSc = Context.CreateSqlContainer($"ATTACH DATABASE '{_dbFile}' AS \"HangFire\""))
        {
            await attachSc.ExecuteNonQueryAsync();
        }

        // Manual install using embedded script
        var ddl = SqliteInstallScript;
        foreach (var stmt in ddl.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.IsNullOrWhiteSpace(stmt) || stmt.StartsWith("--")) continue;
            await using var sc = Context.CreateSqlContainer(stmt);
            await sc.ExecuteNonQueryAsync();
        }

        Storage.Initialize();
    }

    private static string SqliteInstallScript = @"
CREATE TABLE IF NOT EXISTS ""HangFire"".""Schema"" (
    ""Version"" INTEGER NOT NULL,
    PRIMARY KEY (""Version"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""Job"" (
    ""Id"" INTEGER PRIMARY KEY,
    ""StateId"" INTEGER NULL,
    ""StateName"" TEXT NULL,
    ""InvocationData"" TEXT NOT NULL,
    ""Arguments"" TEXT NOT NULL,
    ""CreatedAt"" TEXT NOT NULL,
    ""ExpireAt"" TEXT NULL
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""State"" (
    ""Id"" INTEGER PRIMARY KEY,
    ""JobId"" INTEGER NOT NULL,
    ""Name"" TEXT NOT NULL,
    ""Reason"" TEXT NULL,
    ""CreatedAt"" TEXT NOT NULL,
    ""Data"" TEXT NULL
);

CREATE INDEX IF NOT EXISTS ""HangFire"".""IX_HangFire_State_JobId"" ON ""State"" (""JobId"");

CREATE TABLE IF NOT EXISTS ""HangFire"".""JobParameter"" (
    ""JobId"" INTEGER NOT NULL,
    ""Name"" TEXT NOT NULL,
    ""Value"" TEXT NULL,
    PRIMARY KEY (""JobId"", ""Name"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""JobQueue"" (
    ""Id"" INTEGER PRIMARY KEY,
    ""Queue"" TEXT NOT NULL,
    ""JobId"" INTEGER NOT NULL,
    ""FetchedAt"" TEXT NULL
);

CREATE INDEX IF NOT EXISTS ""HangFire"".""IX_HangFire_JobQueue_Queue"" ON ""JobQueue"" (""Queue"", ""FetchedAt"");

CREATE TABLE IF NOT EXISTS ""HangFire"".""Server"" (
    ""Id"" TEXT NOT NULL,
    ""Data"" TEXT NULL,
    ""LastHeartbeat"" TEXT NOT NULL,
    PRIMARY KEY (""Id"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""Hash"" (
    ""Key"" TEXT NOT NULL,
    ""Field"" TEXT NOT NULL,
    ""Value"" TEXT NULL,
    ""ExpireAt"" TEXT NULL,
    PRIMARY KEY (""Key"", ""Field"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""List"" (
    ""Key"" TEXT NOT NULL,
    ""Id"" INTEGER NOT NULL DEFAULT (abs(random())),
    ""Value"" TEXT NULL,
    ""ExpireAt"" TEXT NULL,
    PRIMARY KEY (""Key"", ""Id"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""Set"" (
    ""Key"" TEXT NOT NULL,
    ""Value"" TEXT NOT NULL,
    ""Score"" REAL NOT NULL DEFAULT 0.0,
    ""ExpireAt"" TEXT NULL,
    PRIMARY KEY (""Key"", ""Value"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""Counter"" (
    ""Key"" TEXT NOT NULL,
    ""Id"" INTEGER NOT NULL DEFAULT (abs(random())),
    ""Value"" INTEGER NOT NULL,
    ""ExpireAt"" TEXT NULL,
    PRIMARY KEY (""Key"", ""Id"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""AggregatedCounter"" (
    ""Key"" TEXT NOT NULL,
    ""Value"" INTEGER NOT NULL DEFAULT 0,
    ""ExpireAt"" TEXT NULL,
    PRIMARY KEY (""Key"")
);

CREATE TABLE IF NOT EXISTS ""HangFire"".""hf_lock"" (
    ""resource""   TEXT    NOT NULL,
    ""owner_id""   TEXT    NOT NULL,
    ""expires_at"" TEXT    NOT NULL,
    ""version""    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (""resource"")
);

INSERT OR IGNORE INTO ""HangFire"".""Schema"" (""Version"") VALUES (9);
";

    public async Task DisposeAsync()
    {
        Context?.Dispose();
        if (File.Exists(_dbFile))
        {
            try { File.Delete(_dbFile); } catch { }
        }
    }

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

[CollectionDefinition("SqliteStress")]
public class SqliteStressCollection : ICollectionFixture<SqliteStressFixture> { }
