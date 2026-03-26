using System;
using System.Data;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;
using pengdows.crud.exceptions;

namespace pengdows.hangfire.gateways;

public sealed class DistributedLockGateway : TableGateway<DistributedLockRecord, string>, IDistributedLockGateway
{
    // Release template: DELETE FROM hf_lock WHERE resource = @k0 AND owner_id = @ownerId
    // Built once from TableGateway.BuildDelete (reuses its cached PK clause) + appended owner guard.
    // Clone() per call; only string params so no dialect DateTime-conversion concern.
    private readonly ISqlContainer _releaseTemplate;

    public DistributedLockGateway(IDatabaseContext context) : base(context)
    {
        _releaseTemplate = BuildReleaseTemplate();
    }

    // ── INSERT path ───────────────────────────────────────────────────────────
    // CreateAsync from TableGateway uses its own cached insert template.

    public async Task<(DistributedLockRecord? Record, bool WasSteal)> TryAcquireAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf)
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
            await CreateAsync(record);
            return (record, false);
        }
        catch (UniqueConstraintViolationException)
        {
            var stolen = await TryUpdateExpiredAsync(resource, ownerId, expiresAt, asOf);
            return stolen ? (record, true) : (null, false);
        }
    }

    // ── Steal path ────────────────────────────────────────────────────────────
    // Dynamic SQL — per-connection MaybePrepareCommand handles statement caching automatically.

    private async Task<bool> TryUpdateExpiredAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf)
    {
        await using var sc = Context.CreateSqlContainer();
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
    // Dynamic SQL — per-connection MaybePrepareCommand handles statement caching automatically.

    public async Task<bool> TryRenewAsync(
        string resource, string ownerId, int expectedVersion, DateTime newExpiresAt)
    {
        await using var sc = Context.CreateSqlContainer();
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

    public async Task ReleaseAsync(string resource, string ownerId)
    {
        await using var sc = _releaseTemplate.Clone();
        sc.SetParameterValue("k0",     resource);
        sc.SetParameterValue("ownerId", ownerId);
        await sc.ExecuteNonQueryAsync();
    }

    // ── Template builders ─────────────────────────────────────────────────────

    private ISqlContainer BuildReleaseTemplate()
    {
        // Start from TableGateway's cached DELETE WHERE [resource] = @k0, then
        // extend with the owner guard so we never accidentally release another owner's lock.
        var sc = BuildDelete(string.Empty);
        sc.AppendAnd().AppendName("owner_id").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("ownerId", DbType.String, string.Empty));
        return sc;
    }
}
