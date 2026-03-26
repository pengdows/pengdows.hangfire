# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pengdows.hangfire` is a Hangfire job storage backend built on [pengdows.crud](../pengdows.crud) — a SQL-first, strongly-typed data access layer. It provides cross-database Hangfire storage via explicit SQL and the gateway pattern.

## Build and Test Commands

```bash
# Build
dotnet build pengdows.hangfire.slnx

# Run all tests
dotnet test pengdows.hangfire.slnx

# Run a single test project
dotnet test pengdows.hangfire.tests/pengdows.hangfire.tests.csproj
dotnet test pengdows.hangfire.integration.tests/pengdows.hangfire.integration.tests.csproj

# Run a single test by name
dotnet test --filter "FullyQualifiedName~TestMethodName"

# Pack for release
dotnet pack -c Release
```

**NuGet source:** Local pengdows.crud artifacts are resolved from `../pengdows.crud/artifacts` (see `NuGet.Config`). Build that project first if the package version is not yet published.

## Architecture

### Core Flow

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

### Gateway Pattern

All database access goes through gateway classes in `Gateways/`. Each gateway extends `TableGateway<TEntity, TId>` from pengdows.crud and implements a corresponding interface:

- `JobGateway` / `IJobGateway` — job CRUD, state, expiration
- `JobStateGateway` / `IJobStateGateway` — state history
- `JobQueueGateway` / `IJobQueueGateway` — queue operations and job fetching
- `JobParameterGateway` / `IJobParameterGateway` — job parameters
- `ServerGateway` / `IServerGateway` — server heartbeat tracking
- `HfLockGateway` / `IHfLockGateway` — distributed lock table
- `SetGateway`, `ListGateway`, `HashGateway` — Hangfire data structures
- `CounterGateway`, `AggregatedCounterGateway` — statistics counters

Gateways receive an `IDatabaseContext` (from pengdows.crud) for all SQL execution.

### SQL Safety

All SQL is written using `SqlContainer` from pengdows.crud:
- `AppendQuery()` — literal SQL text
- `AppendName()` / `WrapObjectName()` — quoted identifiers (prevents injection, ensures cross-DB compatibility)
- `AppendParam()` — parameterized values (never string interpolation for values)

```csharp
await using var sc = context.CreateSqlContainer();
sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName)
  .AppendQuery(" SET ExpireAt = ").AppendParam(expireAt)
  .AppendQuery(" WHERE ").AppendQuery(WrappedPrimaryKey).AppendQuery(" = ").AppendParam(id);
return await sc.ExecuteNonQueryAsync();
```

### Transactions

`PengdowsCrudWriteOnlyTransaction` collects a list of `Func<IDatabaseContext, Task>` commands and executes them atomically via `BeginTransactionAsync()` at commit time. Never use `TransactionScope` — always use `BeginTransaction()`.

### Distributed Locking

`PengdowsCrudDistributedLock` inserts a row into `HfLock` with a retry loop. On conflict it sleeps and retries until the deadline.

### Contracts (Interface Seams)

`Contracts/IHangfire*.cs` re-declare Hangfire interface members so the project fails to compile if Hangfire's API changes. These are not abstractions for consumers — they are compile-time trip wires.

### Entity Models

`Models/*.generated.cs` contain POCO entities decorated with pengdows.crud attributes (`[Id]`, `[Column]`, `[Table]`). These map to the database schema defined in `DefaultInstall.sql` (SQL Server) and `Integration.Tests/SqliteInstall.sql` (SQLite).

### Configuration Entry Point

```csharp
GlobalConfiguration.Configuration
    .UsePengdowsCrudStorage(databaseContext, options => {
        options.SchemaName = "hangfire";
        options.AutoPrepareSchema = true;
        options.QueuePollInterval = TimeSpan.FromSeconds(5);
    });
```

## Testing Infrastructure

**Unit tests** (`pengdows.hangfire.tests/`) use `pengdows.crud.fakeDb`'s in-memory mock provider — no real database required. They test SQL generation, parameter binding, and return value handling.

**Integration tests** (`pengdows.hangfire.integration.tests/`) use `SqliteStorageFixture` — an in-memory SQLite database per test collection. `SqliteInstall.sql` contains the schema DDL variant for SQLite. These tests exercise actual SQL execution end-to-end.

## Code Standards (REVIEW_POLICY.md)

This project has strict standards enforced at review. Key hard bans:

- No `TransactionScope` — use `BeginTransactionAsync()`
- No string interpolation for SQL values — always `AppendParam()`
- No unquoted identifiers — always `WrapObjectName()` / `AppendName()`
- No `else` after scope-exiting control flow (`return`, `throw`, `break`)
- Braces required for all control flow bodies
- One statement per line, always
- `await using` / `using` required for all disposable resources (Lampson Rule: explicit ownership)
- `ValueTask` in hot paths

TDD is mandatory: write unit tests before new behavior. Integration tests required for any DB-facing changes. Adversarial inputs (long strings, reserved words, boundary values) must be covered.
