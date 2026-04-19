using System;
using System.Data;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;
using pengdows.crud.exceptions;

namespace pengdows.hangfire.gateways;

public sealed class DistributedLockGateway : TableGateway<DistributedLockRecord, string>, IDistributedLockGateway
{
    public DistributedLockGateway(IDatabaseContext context) : base(context)
    {
    }

    // ── Acquire path ─────────────────────────────────────────────────────────
    // Single UPSERT attempt — no retry loop.
    //
    // Dialect routing:
    //   SupportsMerge (SQL Server, Oracle, Firebird, DuckDB 1.4+, PostgreSQL 15+):
    //     MERGE WITH (HOLDLOCK for SQL Server) ... WHEN MATCHED AND t.expires_at <= @asOf
    //
    //   SupportsInsertOnConflict + SupportsOnConflictWhere (PostgreSQL < 15, CockroachDB):
    //     INSERT ... ON CONFLICT DO UPDATE SET ... WHERE table.expires_at <= @asOf
    //
    //   SupportsInsertOnConflict only (SQLite, DuckDB < 1.4):
    //     Two-phase: UPDATE WHERE expires_at <= @asOf, then INSERT ... ON CONFLICT DO NOTHING.
    //
    //   Fallback (MySQL, unknown):
    //     INSERT-first; catch UniqueConstraintViolationException; attempt UPDATE WHERE expired.
    //
    // Returns true when the row was inserted or stolen; false when the lock is actively held.

    public Task<bool> TryAcquireAsync(string resource, string ownerId, DateTime expiresAt, DateTime asOf)
        => TryAcquireAsync(resource, ownerId, expiresAt, asOf, null);

    public async Task<bool> TryAcquireAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var dsi     = ctx.DataSourceInfo;
        var dialect = ctx.Dialect;

        // PostgreSQL's MERGE is not safe under concurrent inserts — two sessions can both
        // see WHEN NOT MATCHED and race to INSERT, producing a unique constraint violation.
        // Use ON CONFLICT DO UPDATE instead, which is truly atomic for PostgreSQL.
        var isPostgres = dsi.Product == pengdows.crud.enums.SupportedDatabase.PostgreSql;

        // Firebird's MERGE requires CAST for every untyped parameter in the USING SELECT —
        // the driver cannot infer DbType.String/DateTime from a bare SELECT expression.
        // Route Firebird to the INSERT-first fallback instead (same path as MySQL).
        var isFirebird = dsi.Product == pengdows.crud.enums.SupportedDatabase.Firebird;

        if (dsi.SupportsMerge && !isPostgres && !isFirebird)
        {
            return await TryAcquireMergeAsync(resource, ownerId, expiresAt, asOf, ctx);
        }

        if (dsi.SupportsInsertOnConflict)
        {
            if (dialect.SupportsOnConflictWhere)
            {
                // PostgreSQL < 15, CockroachDB: ON CONFLICT DO UPDATE WHERE.
                return await TryAcquireOnConflictWhereAsync(resource, ownerId, expiresAt, asOf, ctx);
            }

            // SQLite / DuckDB < 1.4: no WHERE predicate on ON CONFLICT DO UPDATE.
            return await TryAcquireTwoPhaseAsync(resource, ownerId, expiresAt, asOf, ctx);
        }

        // MySQL / unknown: INSERT-first with exception catch.
        return await TryAcquireInsertFirstAsync(resource, ownerId, expiresAt, asOf, ctx);
    }

    // ── SQL Server / Oracle / Firebird / DuckDB 1.4+ / PostgreSQL 15+ — MERGE ─
    //
    // Dialect quirks handled here:
    //   Oracle  — target/source alias must NOT use AS keyword; USING subquery needs FROM DUAL;
    //             ON predicate must be wrapped in parentheses (via RenderMergeOnClause).
    //   Firebird — USING subquery needs FROM RDB$DATABASE for a constant SELECT.
    //   SQL Server — standard AS aliases; no FROM needed; HOLDLOCK hint appended to target.

    private async Task<bool> TryAcquireMergeAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext ctx)
    {
        await using var sc = ctx.CreateSqlContainer();
        var dialect = ctx.Dialect;
        var product = ctx.DataSourceInfo.Product;

        // tp: prepended to column names in SET clause that must reference the target row.
        var tp = dialect.MergeUpdateRequiresTargetAlias ? "t." : "";

        // Pre-wrap column names so they are quoted correctly for this dialect.
        var wResource  = dialect.WrapSimpleName("resource");
        var wOwnerId   = dialect.WrapSimpleName("owner_id");
        var wExpiresAt = dialect.WrapSimpleName("expires_at");
        var wVersion   = dialect.WrapSimpleName("version");

        // Oracle MERGE syntax forbids the AS keyword before both the target and source aliases.
        // Other databases (SQL Server, Firebird, DuckDB) accept AS alias.
        var aliasAs = product == pengdows.crud.enums.SupportedDatabase.Oracle ? " " : " AS ";

        // Oracle requires FROM DUAL; Firebird requires FROM RDB$DATABASE for constant SELECTs.
        var fromClause = product switch
        {
            pengdows.crud.enums.SupportedDatabase.Oracle   => " FROM DUAL",
            pengdows.crud.enums.SupportedDatabase.Firebird => " FROM RDB$DATABASE",
            _                                              => ""
        };

        sc.AppendQuery("MERGE INTO ").AppendQuery(WrappedTableName);
        if (product == pengdows.crud.enums.SupportedDatabase.SqlServer)
        {
            sc.AppendQuery(" WITH (HOLDLOCK)");
        }

        // Parameter names avoid Oracle reserved words: "resource" is reserved in Oracle.
        sc.AppendQuery($"{aliasAs}t USING (SELECT ");
        sc.AppendParam(sc.AddParameterWithValue("lockRes",   DbType.String,   resource))
          .AppendQuery($" AS {wResource}, ");
        sc.AppendParam(sc.AddParameterWithValue("lockOwner", DbType.String,   ownerId))
          .AppendQuery($" AS {wOwnerId}, ");
        sc.AppendParam(sc.AddParameterWithValue("lockExp",   DbType.DateTime, expiresAt))
          .AppendQuery($" AS {wExpiresAt}");

        // ON condition: dialect.RenderMergeOnClause wraps in parens for Oracle.
        var onPredicate = dialect.RenderMergeOnClause($"t.{wResource} = s.{wResource}");
        sc.AppendQuery($"{fromClause}){aliasAs}s ON {onPredicate}");

        // WHEN MATCHED must come before WHEN NOT MATCHED in Oracle.
        // SQL Server and DuckDB accept either order; keeping MATCHED first is portable.
        //
        // Oracle does not support AND condition on WHEN MATCHED — the expiry filter must
        // go into a WHERE clause on the UPDATE body.
        // SQL Server and DuckDB do not support WHERE inside WHEN MATCHED UPDATE — the
        // filter must go into the WHEN MATCHED AND condition.
        if (product == pengdows.crud.enums.SupportedDatabase.Oracle)
        {
            sc.AppendQuery($" WHEN MATCHED THEN UPDATE SET {tp}{wOwnerId} = s.{wOwnerId}, {tp}{wExpiresAt} = s.{wExpiresAt}, {tp}{wVersion} = 1");
            sc.AppendQuery($" WHERE t.{wExpiresAt} <= ");
            sc.AppendParam(sc.AddParameterWithValue("asOf", DbType.DateTime, asOf));
        }
        else
        {
            sc.AppendQuery($" WHEN MATCHED AND t.{wExpiresAt} <= ");
            sc.AppendParam(sc.AddParameterWithValue("asOf", DbType.DateTime, asOf));
            sc.AppendQuery($" THEN UPDATE SET {tp}{wOwnerId} = s.{wOwnerId}, {tp}{wExpiresAt} = s.{wExpiresAt}, {tp}{wVersion} = 1");
        }

        // INSERT when no existing row matches.
        sc.AppendQuery($" WHEN NOT MATCHED THEN INSERT ({wResource}, {wOwnerId}, {wExpiresAt}, {wVersion})");
        sc.AppendQuery($" VALUES (s.{wResource}, s.{wOwnerId}, s.{wExpiresAt}, 1)");

        // SQL Server requires MERGE to be terminated with a semicolon; Oracle rejects it.
        if (product == pengdows.crud.enums.SupportedDatabase.SqlServer)
        {
            sc.AppendQuery(";");
        }

        return await sc.ExecuteNonQueryAsync() == 1;
    }

    // ── PostgreSQL < 15 / CockroachDB — ON CONFLICT DO UPDATE WHERE ───────────

    private async Task<bool> TryAcquireOnConflictWhereAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext ctx)
    {
        await using var sc = ctx.CreateSqlContainer();
        var dialect  = ctx.Dialect;
        var incoming = dialect.UpsertIncomingColumn;

        var wResource  = dialect.WrapSimpleName("resource");
        var wOwnerId   = dialect.WrapSimpleName("owner_id");
        var wExpiresAt = dialect.WrapSimpleName("expires_at");
        var wVersion   = dialect.WrapSimpleName("version");

        sc.AppendQuery("INSERT INTO ").AppendQuery(WrappedTableName)
          .AppendQuery($" ({wResource}, {wOwnerId}, {wExpiresAt}, {wVersion}) VALUES (");
        sc.AppendParam(sc.AddParameterWithValue("resource",  DbType.String,   resource));
        sc.AppendComma();
        sc.AppendParam(sc.AddParameterWithValue("ownerId",   DbType.String,   ownerId));
        sc.AppendComma();
        sc.AppendParam(sc.AddParameterWithValue("expiresAt", DbType.DateTime, expiresAt));
        sc.AppendQuery($", 1) ON CONFLICT ({wResource}) DO UPDATE SET {wOwnerId} = ");
        sc.AppendQuery(incoming("owner_id"));
        sc.AppendQuery($", {wExpiresAt} = ");
        sc.AppendQuery(incoming("expires_at"));
        sc.AppendQuery($", {wVersion} = 1 WHERE ");
        sc.AppendQuery(WrappedTableName).AppendQuery($".{wExpiresAt} <= ");
        sc.AppendParam(sc.AddParameterWithValue("asOf", DbType.DateTime, asOf));

        return await sc.ExecuteNonQueryAsync() == 1;
    }

    // ── SQLite / DuckDB < 1.4 — two-phase UPDATE then INSERT ON CONFLICT DO NOTHING

    private async Task<bool> TryAcquireTwoPhaseAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext ctx)
    {
        // Phase 1: steal an expired row atomically.
        if (await TryUpdateExpiredAsync(resource, ownerId, expiresAt, asOf, ctx))
        {
            return true;
        }

        // Phase 2: try to insert a new row; ignore if another writer just inserted it.
        await using var sc = ctx.CreateSqlContainer();
        var dialect = ctx.Dialect;

        var wResource  = dialect.WrapSimpleName("resource");
        var wOwnerId   = dialect.WrapSimpleName("owner_id");
        var wExpiresAt = dialect.WrapSimpleName("expires_at");
        var wVersion   = dialect.WrapSimpleName("version");

        sc.AppendQuery("INSERT INTO ").AppendQuery(WrappedTableName)
          .AppendQuery($" ({wResource}, {wOwnerId}, {wExpiresAt}, {wVersion}) VALUES (");
        sc.AppendParam(sc.AddParameterWithValue("resource",  DbType.String,   resource));
        sc.AppendComma();
        sc.AppendParam(sc.AddParameterWithValue("ownerId",   DbType.String,   ownerId));
        sc.AppendComma();
        sc.AppendParam(sc.AddParameterWithValue("expiresAt", DbType.DateTime, expiresAt));
        sc.AppendQuery($", 1) ON CONFLICT ({wResource}) DO NOTHING");

        return await sc.ExecuteNonQueryAsync() == 1;
    }

    // ── MySQL / unknown fallback — INSERT-first with exception ────────────────

    private async Task<bool> TryAcquireInsertFirstAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext ctx)
    {
        var record = new DistributedLockRecord
        {
            Resource  = resource,
            OwnerId   = ownerId,
            ExpiresAt = expiresAt,
            Version   = 1
        };

        try
        {
            await CreateAsync(record, ctx);
            return true;
        }
        catch (UniqueConstraintViolationException)
        {
            return await TryUpdateExpiredAsync(resource, ownerId, expiresAt, asOf, ctx);
        }
    }

    // ── Shared steal helper ───────────────────────────────────────────────────

    private async Task<bool> TryUpdateExpiredAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf, IDatabaseContext ctx)
    {
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("owner_id").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("ownerId", DbType.String, ownerId));
        sc.AppendComma();
        sc.AppendName("expires_at").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("expiresAt", DbType.DateTime, expiresAt));
        sc.AppendComma();
        sc.AppendName("version").AppendQuery(" = 1");
        sc.AppendWhere();
        sc.AppendName("resource").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("lockRes", DbType.String, resource));
        sc.AppendAnd().AppendName("expires_at").AppendQuery(" <= ")
          .AppendParam(sc.AddParameterWithValue("asOf", DbType.DateTime, asOf));
        return await sc.ExecuteNonQueryAsync() == 1;
    }

    // ── Renew ─────────────────────────────────────────────────────────────────

    public Task<bool> TryRenewAsync(string resource, string ownerId, int expectedVersion, DateTime newExpiresAt)
        => TryRenewAsync(resource, ownerId, expectedVersion, newExpiresAt, null);

    public async Task<bool> TryRenewAsync(
        string resource, string ownerId, int expectedVersion, DateTime newExpiresAt, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("expires_at").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("newExpires", DbType.DateTime, newExpiresAt));
        sc.AppendComma();
        sc.AppendName("version").AppendQuery(" = ").AppendName("version").AppendQuery(" + 1");
        sc.AppendWhere();
        sc.AppendName("resource").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("lockRes", DbType.String, resource));
        sc.AppendAnd().AppendName("owner_id").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("ownerId", DbType.String, ownerId));
        sc.AppendAnd().AppendName("version").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("lockVer", DbType.Int32, expectedVersion));
        return await sc.ExecuteNonQueryAsync() == 1;
    }

    // ── Release ───────────────────────────────────────────────────────────────

    public Task ReleaseAsync(string resource, string ownerId) => ReleaseAsync(resource, ownerId, null);

    public async Task ReleaseAsync(string resource, string ownerId, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = BuildReleaseTemplate(resource, ownerId, ctx);
        await sc.ExecuteNonQueryAsync();
    }

    // ── Template builders ─────────────────────────────────────────────────────

    private ISqlContainer BuildReleaseTemplate(string resource, string ownerId, IDatabaseContext context)
    {
        var sc = BuildDelete(resource, context);
        sc.AppendAnd().AppendName("owner_id").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("ownerId", DbType.String, ownerId));
        return sc;
    }
}
