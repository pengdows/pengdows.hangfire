# pengdows.hangfire

SQL-first Hangfire job storage built on [pengdows.crud](https://github.com/pengdows/pengdows.crud) — a strongly-typed, cross-database data access layer for .NET.

## Supported Databases

| Database     | Notes                       |
|--------------|-----------------------------|
| PostgreSQL   |                             |
| SQL Server   |                             |
| Oracle       |                             |
| Firebird     |                             |
| CockroachDB  |                             |
| MariaDB      |                             |
| MySQL        |                             |
| SQLite       |                             |
| DuckDB       |                             |
| YugabyteDB   |                             |
| TiDB         |                             |

> **Snowflake is not supported.** `pengdows.crud` includes a Snowflake driver, but Snowflake is designed for analytical workloads — columnar storage, warehouse-level concurrency, and high per-query latency. Hangfire requires row-level locking, low-latency queue polling, high-frequency small writes and deletes, and reliable distributed lock semantics. These requirements are fundamentally at odds with Snowflake's architecture.

### SQLite and DuckDB in Production

SQLite and DuckDB are fully supported for production use. `pengdows.crud`'s **SingleWriter mode** serializes all write operations, eliminating the `SQLITE_BUSY` / `database is locked` errors that plague naive multi-threaded use of these engines. The **PoolGovernor** prevents connection pool saturation by bounding concurrent database access, so Hangfire's background threads cannot starve the rest of the application.

If your application already uses `pengdows.crud` with the same `IDatabaseContext`, Hangfire shares that context and its governance. Pool exhaustion from runaway background jobs becomes a non-issue.

If the rest of your application does not use `pengdows.crud`, you can still isolate Hangfire's impact by giving it a dedicated connection string pointing to the same database. ADO.NET pools connections per unique connection string, so Hangfire gets its own pool and cannot compete with your application's connections regardless of load.

## Requirements

- .NET 8.0
- Hangfire.Core ≥ 1.8.x
- pengdows.crud 2.x
- An ADO.NET driver for your chosen database

## Installation

```shell
dotnet add package pengdows.hangfire
```

## Quick Start

```csharp
using pengdows.crud;
using Hangfire;

// Create your pengdows.crud IDatabaseContext for your target database
IDatabaseContext db = DatabaseContextFactory.Create(connectionString, dbType);

GlobalConfiguration.Configuration
    .UsePengdowsCrudStorage(db, options =>
    {
        options.SchemaName = "hangfire";
        options.AutoPrepareSchema = true;
        options.QueuePollInterval = TimeSpan.FromSeconds(5);
    });
```

`AutoPrepareSchema = true` (the default) creates all required tables on first run. Set it to `false` if you manage schema migrations yourself.

## Configuration

All options are set via `PengdowsCrudStorageOptions`:

| Option                       | Default    | Description                                                         |
|------------------------------|------------|---------------------------------------------------------------------|
| `SchemaName`                 | `hangfire` | Database schema to place all Hangfire tables in                     |
| `AutoPrepareSchema`          | `true`     | Create schema tables on initialization if they do not exist         |
| `QueuePollInterval`          | 5 sec      | How long a worker waits between queue polls when idle               |
| `InvisibilityTimeout`        | 5 min      | How long a fetched job is invisible to other workers before requeue |
| `ServerHeartbeatInterval`    | 30 sec     | How often servers write a heartbeat to the `Server` table           |
| `JobExpirationCheckInterval` | 30 min     | How often the expiration manager purges expired jobs                |
| `CountersAggregateInterval`  | 5 min      | How often fine-grained counters are rolled up into aggregates       |
| `DistributedLockTtl`         | 5 min      | How long a distributed lock row lives before it can be force-taken  |
| `DistributedLockRetryDelay`  | 50 ms      | Base sleep between distributed lock acquire attempts                |
| `DistributedLockRetryJitter` | `true`     | Randomize retry sleep to prevent thundering-herd on lock contention |

## Architecture

All database access goes through gateway classes that extend `TableGateway<TEntity, TId>` from pengdows.crud. Each gateway owns exactly one table and exposes a typed interface.

```
Hangfire → PengdowsCrudJobStorage
              ├── PengdowsCrudConnection        (IStorageConnection)
              │     └── PengdowsCrudWriteOnlyTransaction  (IWriteOnlyTransaction)
              ├── PengdowsCrudMonitoringApi      (IMonitoringApi)
              ├── PengdowsCrudDistributedLock    (IDistributedLock)
              └── Background processes
                    ├── ExpirationManager
                    └── CountersAggregator
```

SQL is built exclusively with `SqlContainer` from pengdows.crud — quoted identifiers via `AppendName()` / `WrapObjectName()`, parameterized values via `AppendParam()`. No string interpolation for SQL values, ever.

Transactions are handled by collecting `Func<IDatabaseContext, Task>` commands in `PengdowsCrudWriteOnlyTransaction` and executing them atomically via `BeginTransactionAsync()` at commit time.

## Schema Management

By default (`AutoPrepareSchema = true`) the schema is installed automatically on first use.

For manual management, the DDL is available as embedded resources:

- `DefaultInstall.sql` — SQL Server dialect (ships with the package)
- `Liquibase/` — versioned migrations for all supported databases

The schema is versioned. Running against an existing schema applies only the missing migrations.

## Building and Testing

```bash
# Build
dotnet build pengdows.hangfire.slnx

# Run all tests
dotnet test pengdows.hangfire.slnx

# Unit tests only (no database required — uses pengdows.crud.fakeDb)
dotnet test pengdows.hangfire.tests/pengdows.hangfire.tests.csproj

# Integration tests (uses in-memory SQLite)
dotnet test pengdows.hangfire.integration.tests/pengdows.hangfire.integration.tests.csproj

# Pack for release
dotnet pack -c Release
```

Unit tests use `pengdows.crud.fakeDb` — an in-memory mock provider that exercises SQL generation, parameter binding, and return value handling without a real database. Integration tests use SQLite via an in-process fixture.
