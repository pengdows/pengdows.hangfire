using System.Data.Common;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using FirebirdSql.Data.FirebirdClient;
using DuckDB.NET.Data;
using pengdows.crud;
using pengdows.crud.configuration;
using pengdows.crud.enums;
using pengdows.crud.metrics;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.PostgreSql;
using Xunit;

namespace pengdows.hangfire.integration.tests;

// ── Helper: run per-statement DDL from embedded SQL resource ────────────────

internal static class SchemaHelper
{
    private static readonly System.Reflection.Assembly _assembly =
        typeof(SchemaHelper).Assembly;

    internal static async Task InstallFromResourceAsync(IDatabaseContext ctx, string resourceName)
    {
        var fullName = $"pengdows.hangfire.integration.tests.{resourceName}";
        await using var stream = _assembly.GetManifestResourceStream(fullName)
            ?? throw new InvalidOperationException($"Embedded resource '{fullName}' not found.");
        using var reader = new StreamReader(stream);
        var ddl = await reader.ReadToEndAsync();
        // Strip single-line comments before splitting so that semicolons inside
        // comment text (e.g. "-- tables owned by the user; no schema prefix") do not
        // produce spurious statement fragments that fail as invalid SQL.
        var stripped = System.Text.RegularExpressions.Regex.Replace(ddl, @"--[^\n]*", "");
        foreach (var stmt in stripped.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.IsNullOrWhiteSpace(stmt)) continue;
            await using var sc = ctx.CreateSqlContainer(stmt);
            await sc.ExecuteNonQueryAsync();
        }
    }
}

// ── Container readiness helper ───────────────────────────────────────────────

internal static class ContainerHelper
{
    internal static async Task WaitForConnectionAsync(
        DbProviderFactory factory,
        string connectionString,
        int timeoutSeconds = 120)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var conn = factory.CreateConnection()
                    ?? throw new InvalidOperationException("Factory returned null connection.");
                conn.ConnectionString = connectionString;
                await conn.OpenAsync();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(1_000);
            }
        }
        throw new TimeoutException(
            $"Database not ready after {timeoutSeconds}s: {last?.Message}", last);
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

public class SqliteFixture : StorageFixture
{
    private readonly string _dbFile = $"hangfire_{Guid.NewGuid():N}.db";
    protected override string ConnectionString => $"Data Source={_dbFile}";
    protected override DbProviderFactory Factory => SqliteFactory.Instance;

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        Console.WriteLine(
            $"[sqlite-fixture] file={_dbFile} product={Context.Product} mode={Context.ConnectionMode} cs={ConnectionString}");
        Console.WriteLine(((PengdowsCrudMonitoringApi)Storage.GetMonitoringApi()).GetDatabaseMetricGrid());
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "SqliteInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        if (File.Exists(_dbFile))
        {
            try { File.Delete(_dbFile); } catch { }
        }
    }
}

public class PostgresFixture : StorageFixture
{
    private PostgreSqlContainer _container = null!;
    private string? _connectionString;

    static PostgresFixture()
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
    }

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => NpgsqlFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16")
            .WithDatabase("hangfire")
            .WithUsername("hangfire")
            .WithPassword("password")
            .Build();
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "PostgresInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class SqlServerFixture : StorageFixture
{
    private MsSqlContainer _container = null!;
    private string? _connectionString;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => SqlClientFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
        await base.InitializeAsync();
    }

    protected override async Task InstallSchemaAsync()
    {
        var installer = new PengdowsCrudSchemaInstaller(Context);
        await installer.InstallAsync();
    }

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class MySqlFixture : StorageFixture
{
    private MySqlContainer _container = null!;
    private string? _connectionString;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => MySqlConnectorFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new MySqlBuilder()
            .WithImage("mysql:8.0")
            .WithDatabase("HangFire")
            .WithPassword("password")
            .Build();
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString() + ";AllowUserVariables=true";
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "MySqlInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class OracleFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    private const string _password = "Oracle_1";
    private const string _sid = "FREEPDB1";
    private const int _oraclePort = 1521;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => OracleClientFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("gvenzl/oracle-free:slim")
            .WithEnvironment("ORACLE_PASSWORD", _password)
            .WithEnvironment("ORACLE_CHARACTERSET", "AL32UTF8")
            .WithPortBinding(_oraclePort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(_oraclePort))
            .Build();
        await _container.StartAsync();

        var hostPort = _container.GetMappedPublicPort(_oraclePort);
        var tns = $"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT={hostPort}))(CONNECT_DATA=(SERVICE_NAME={_sid})))";
        var systemConnStr = $"User Id=system;Password={_password};Data Source={tns}";

        // Oracle needs extra startup time beyond port availability
        await ContainerHelper.WaitForConnectionAsync(OracleClientFactory.Instance, systemConnStr, timeoutSeconds: 300);

        // Create the "HangFire" schema user and grant minimum required privileges
        await using var adminCtx = new DatabaseContext(systemConnStr, OracleClientFactory.Instance);
        foreach (var sql in new[]
        {
            @"CREATE USER ""HangFire"" IDENTIFIED BY " + _password,
            @"GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO ""HangFire""",
        })
        {
            await using var sc = adminCtx.CreateSqlContainer(sql);
            await sc.ExecuteNonQueryAsync();
        }

        // Connect as "HangFire" (double-quoted in the value so Oracle treats it as
        // a case-sensitive identifier — single-quote delimiters preserve the inner quotes).
        _connectionString = $"User Id='\"HangFire\"';Password={_password};Data Source={tns}";
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "OracleInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class FirebirdFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    private const string _password = "password";
    private const string _dbPath = "/var/lib/firebird/data/hangfire.fdb";
    private const int _fbPort = 3050;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => FirebirdClientFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("firebirdsql/firebird")
            .WithPortBinding(_fbPort, true)
            .WithEnvironment("ISC_PASSWORD", _password)
            .WithEnvironment("FIREBIRD_ROOT_PASSWORD", _password)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(_fbPort))
            .Build();
        await _container.StartAsync();

        var hostPort = _container.GetMappedPublicPort(_fbPort);
        var sysdbaConnStr = new FbConnectionStringBuilder
        {
            DataSource = "localhost",
            Port = hostPort,
            Database = _dbPath,
            UserID = "SYSDBA",
            Password = _password,
            Charset = "UTF8",
        }.ToString();

        // Create the database (retry until the Firebird server is ready)
        var deadline = DateTime.UtcNow.AddSeconds(120);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await FbConnection.CreateDatabaseAsync(sysdbaConnStr);
                last = null;
                break;
            }
            catch (Exception ex) { last = ex; await Task.Delay(2_000); }
        }
        if (last != null)
        {
            throw new TimeoutException($"Firebird not ready after 120s: {last.Message}", last);
        }

        // FirebirdDialect.SupportsNamespaces = false, so tables have no schema prefix.
        // Connecting as SYSDBA is sufficient — no dedicated HangFire user needed.
        _connectionString = sysdbaConnStr;
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "FirebirdInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class CockroachDbFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    static CockroachDbFixture()
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
    }

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => NpgsqlFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("cockroachdb/cockroach:latest")
            .WithCommand("start-single-node", "--insecure")
            .WithPortBinding(26257, true)
            .Build();
        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(26257);
        var adminConnStr = $"Host=localhost;Port={port};Username=root;Database=defaultdb;SSL Mode=Disable";
        await ContainerHelper.WaitForConnectionAsync(NpgsqlFactory.Instance, adminConnStr);
        await using var adminCtx = new DatabaseContext(adminConnStr, NpgsqlFactory.Instance);
        await using var sc = adminCtx.CreateSqlContainer("CREATE DATABASE IF NOT EXISTS hangfire");
        await sc.ExecuteNonQueryAsync();
        _connectionString = $"Host=localhost;Port={port};Username=root;Database=hangfire;SSL Mode=Disable";
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "PostgresInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class MariaDbFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => MySqlConnectorFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("mariadb:11.4")
            .WithEnvironment("MARIADB_ROOT_PASSWORD", "password")
            .WithEnvironment("MARIADB_DATABASE", "HangFire")
            .WithPortBinding(3306, true)
            .Build();
        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(3306);
        _connectionString = $"Server=localhost;Port={port};Database=HangFire;Uid=root;Pwd=password;AllowUserVariables=true";
        await ContainerHelper.WaitForConnectionAsync(MySqlConnectorFactory.Instance, _connectionString);
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "MySqlInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class DuckDbFixture : StorageFixture
{
    private readonly string _dbFile = $"hangfire_{Guid.NewGuid():N}.duckdb";
    protected override string ConnectionString => $"DataSource={_dbFile}";
    protected override DbProviderFactory Factory => DuckDBClientFactory.Instance;

    public override async Task InitializeAsync()
    {
        // By using a file-based path and no explicit DbMode, 
        // pengdows.crud will auto-select SingleWriter (DbMode.Best).
        Context = new DatabaseContext(
            new DatabaseContextConfiguration
            {
                ConnectionString = ConnectionString,
                EnableMetrics = true,
                MetricsOptions = new MetricsOptions { EnableApproxPercentiles = true }
            },
            DuckDBClientFactory.Instance);
        await InstallSchemaAsync();
        Storage = new PengdowsCrudJobStorage(Context,
            new PengdowsCrudStorageOptions { AutoPrepareSchema = false });
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "DuckDbInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        if (File.Exists(_dbFile))
        {
            try { File.Delete(_dbFile); } catch { }
        }
    }
}

public class YugabyteDbFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    static YugabyteDbFixture()
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
    }

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => NpgsqlFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("yugabytedb/yugabyte:latest")
            .WithCommand("bin/yugabyted", "start", "--daemon=false")
            .WithPortBinding(5433, true)
            .Build();
        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(5433);
        _connectionString = $"Host=localhost;Port={port};Username=yugabyte;Database=yugabyte;SSL Mode=Disable";
        await ContainerHelper.WaitForConnectionAsync(NpgsqlFactory.Instance, _connectionString, timeoutSeconds: 180);
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "PostgresInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public class TiDbFixture : StorageFixture
{
    private IContainer _container = null!;
    private string? _connectionString;

    protected override string ConnectionString => _connectionString!;
    protected override DbProviderFactory Factory => MySqlConnectorFactory.Instance;

    public override async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("pingcap/tidb:latest")
            .WithPortBinding(4000, true)
            .Build();
        await _container.StartAsync();
        var port = _container.GetMappedPublicPort(4000);
        var adminConnStr = $"Server=localhost;Port={port};Uid=root;Pwd=;AllowUserVariables=true";
        await ContainerHelper.WaitForConnectionAsync(MySqlConnectorFactory.Instance, adminConnStr);
        await using var adminCtx = new DatabaseContext(adminConnStr, MySqlConnectorFactory.Instance);
        await using var sc = adminCtx.CreateSqlContainer("CREATE DATABASE IF NOT EXISTS `HangFire`");
        await sc.ExecuteNonQueryAsync();
        _connectionString = $"Server=localhost;Port={port};Database=HangFire;Uid=root;Pwd=;AllowUserVariables=true";
        await base.InitializeAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "MySqlInstall.sql");

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _container.DisposeAsync();
    }
}

// ── Collection definitions ───────────────────────────────────────────────────

[CollectionDefinition("Sqlite")]
public class SqliteCollection : ICollectionFixture<SqliteFixture> { }

[CollectionDefinition("PostgreSql")]
public class PostgreSqlCollection : ICollectionFixture<PostgresFixture> { }

[CollectionDefinition("SqlServer")]
public class SqlServerCollection : ICollectionFixture<SqlServerFixture> { }

[CollectionDefinition("MySql")]
public class MySqlCollection : ICollectionFixture<MySqlFixture> { }

[CollectionDefinition("Oracle")]
public class OracleCollection : ICollectionFixture<OracleFixture> { }

[CollectionDefinition("Firebird")]
public class FirebirdCollection : ICollectionFixture<FirebirdFixture> { }

[CollectionDefinition("CockroachDb")]
public class CockroachDbCollection : ICollectionFixture<CockroachDbFixture> { }

[CollectionDefinition("MariaDb")]
public class MariaDbCollection : ICollectionFixture<MariaDbFixture> { }

[CollectionDefinition("DuckDb")]
public class DuckDbCollection : ICollectionFixture<DuckDbFixture> { }

[CollectionDefinition("YugabyteDb")]
public class YugabyteDbCollection : ICollectionFixture<YugabyteDbFixture> { }

[CollectionDefinition("TiDb")]
public class TiDbCollection : ICollectionFixture<TiDbFixture> { }
