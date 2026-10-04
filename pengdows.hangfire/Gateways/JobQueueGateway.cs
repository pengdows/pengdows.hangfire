using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.exceptions;

namespace pengdows.hangfire.gateways;

public sealed class JobQueueGateway : TableGateway<JobQueue, long>, IJobQueueGateway
{
    private enum ClaimLockMode
    {
        None,
        SqlServer,
        PostgreSql
    }

    private const int CandidateBatchSize = 32;
    private const int MaxTransientClaimAttempts = 3;
    private readonly Func<DateTime> _utcNow;
    public JobQueueGateway(IDatabaseContext context, Func<DateTime> utcNow) : base(context)
        => _utcNow = utcNow ?? throw new ArgumentNullException(nameof(utcNow));

    public Task<int> AcknowledgeAsync(long jobId, string queue) => AcknowledgeAsync(jobId, queue, null, null);

    public Task<int> AcknowledgeAsync(long jobId, string queue, string fetchToken)
        => AcknowledgeAsync(jobId, queue, fetchToken, null);

    public Task<int> AcknowledgeAsync(long jobId, string queue, IDatabaseContext context)
        => AcknowledgeAsync(jobId, queue, null, context);

    private async Task<int> AcknowledgeAsync(long jobId, string queue, string? fetchToken, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendAnd().AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        if (fetchToken != null)
        {
            sc.AppendAnd().AppendName("FetchToken").AppendEquals()
              .AppendParam(sc.AddParameterWithValue("fetchToken", DbType.String, fetchToken));
        }
        return await sc.ExecuteNonQueryAsync();
    }

    public Task<int> RequeueAsync(long jobId, string queue) => RequeueAsync(jobId, queue, null, null);

    public Task<int> RequeueAsync(long jobId, string queue, string fetchToken)
        => RequeueAsync(jobId, queue, fetchToken, null);

    public Task<int> RequeueAsync(long jobId, string queue, IDatabaseContext context)
        => RequeueAsync(jobId, queue, null, context);

    private async Task<int> RequeueAsync(long jobId, string queue, string? fetchToken, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendQuery(" = NULL");
        sc.AppendComma().AppendName("FetchToken").AppendQuery(" = NULL WHERE ");
        sc.AppendName("JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendAnd().AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        if (fetchToken != null)
        {
            sc.AppendAnd().AppendName("FetchToken").AppendEquals()
              .AppendParam(sc.AddParameterWithValue("fetchToken", DbType.String, fetchToken));
        }
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> KeepAliveAsync(long jobId, string queue, string fetchToken, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, _utcNow()));
        sc.AppendWhere();
        sc.AppendName("JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendAnd().AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        sc.AppendAnd().AppendName("FetchToken").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("fetchToken", DbType.String, fetchToken));
        return await sc.ExecuteNonQueryAsync();
    }

    public Task<int> RequeueStaleAsync(DateTime cutoff) => RequeueStaleAsync(cutoff, null);

    public async Task<int> RequeueStaleAsync(DateTime cutoff, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendQuery(" = NULL");
        sc.AppendComma().AppendName("FetchToken").AppendQuery(" = NULL");
        sc.AppendWhere();
        sc.AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" <= ")
          .AppendParam(sc.AddParameterWithValue("cutoff", DbType.DateTime, cutoff));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> CountUnfencedFetchedAsync(IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("SELECT COUNT(*) FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        sc.AppendAnd().AppendName("FetchToken").AppendQuery(" IS NULL");
        return await sc.ExecuteScalarRequiredAsync<int>();
    }

    public async Task ValidateFetchTokenColumnAsync(IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("SELECT ").AppendName("FetchToken").AppendQuery(" FROM ")
          .AppendQuery(WrappedTableName).AppendQuery(" WHERE 1 = 0");
        await using var reader = await sc.ExecuteReaderAsync();
        while (await reader.ReadAsync()) { }
    }

    public Task<List<string>> GetDistinctQueuesAsync() => GetDistinctQueuesAsync(null);

    public async Task<List<string>> GetDistinctQueuesAsync(IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("SELECT DISTINCT ").AppendName("Queue").AppendQuery(" FROM ").AppendQuery(WrappedTableName);
        await using var reader = await sc.ExecuteReaderAsync();
        var queues = new List<string>();
        while (await reader.ReadAsync())
        {
            queues.Add(reader.GetString(0));
        }
        return queues;
    }

    public Task<List<JobQueue>> GetPagedByQueueAsync(string queue, int from, int count, bool fetched)
        => GetPagedByQueueAsync(queue, from, count, fetched, null);

    public async Task<List<JobQueue>> GetPagedByQueueAsync(string queue, int from, int count, bool fetched, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var sc = BuildBaseRetrieve("q", ctx);
        sc.AppendWhere();
        sc.AppendName("q.Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("q.FetchedAt").AppendQuery(fetched ? " IS NOT NULL" : " IS NULL");
        sc.AppendQuery(" ORDER BY ").AppendName("q.Id").AppendQuery(" ASC");
        ctx.Dialect.AppendPaging(sc.Query, from, count);
        return await LoadListAsync(sc);
    }

    public Task<(long JobId, string Queue, string FetchToken)?> FetchNextJobAsync(string[] queues, CancellationToken ct)
        => FetchNextJobAsync(queues, ct, null);

    public async Task<(long JobId, string Queue, string FetchToken)?> FetchNextJobAsync(string[] queues, CancellationToken ct, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var claimLockMode = GetClaimLockMode(ctx);
        if (claimLockMode != ClaimLockMode.None)
        {
            return await FetchNextJobWithSkipLockedAsync(queues, ct, ctx, claimLockMode);
        }

        foreach (var queue in queues)
        {
            ct.ThrowIfCancellationRequested();

            // Materialize candidates before claiming so SingleConnection contexts do not
            // attempt an UPDATE while this reader still owns the shared connection gate.
            var candidates = await ReadCandidatesAsync(queue, ctx, ClaimLockMode.None, ct);

            foreach (var (id, jobId) in candidates)
            {
                ct.ThrowIfCancellationRequested();
                var fetchToken = Guid.NewGuid().ToString("N");
                if (await TryClaimAsync(id, queue, fetchToken, ct, ctx))
                {
                    return (jobId, queue, fetchToken);
                }
            }
        }

        return null;
    }

    private static ClaimLockMode GetClaimLockMode(IDatabaseContext context)
    {
        if (context.ConnectionMode == DbMode.SingleConnection)
        {
            return ClaimLockMode.None;
        }

        return context.Product switch
        {
            SupportedDatabase.SqlServer => ClaimLockMode.SqlServer,
            SupportedDatabase.PostgreSql => ClaimLockMode.PostgreSql,
            _ => ClaimLockMode.None
        };
    }

    private async Task<(long JobId, string Queue, string FetchToken)?> FetchNextJobWithSkipLockedAsync(
        string[] queues,
        CancellationToken ct,
        IDatabaseContext context,
        ClaimLockMode lockMode)
    {
        await using var transaction = await context.BeginTransactionAsync(cancellationToken: ct);
        foreach (var queue in queues)
        {
            ct.ThrowIfCancellationRequested();
            var candidates = await ReadCandidatesAsync(queue, transaction, lockMode, ct);
            foreach (var (id, jobId) in candidates)
            {
                ct.ThrowIfCancellationRequested();
                var fetchToken = Guid.NewGuid().ToString("N");
                if (await TryClaimAsync(id, queue, fetchToken, ct, transaction))
                {
                    await transaction.CommitAsync(ct);
                    return (jobId, queue, fetchToken);
                }
            }
        }

        await transaction.CommitAsync(ct);
        return null;
    }

    private async Task<List<(long Id, long JobId)>> ReadCandidatesAsync(
        string queue,
        IDatabaseContext context,
        ClaimLockMode lockMode,
        CancellationToken ct)
    {
        var candidates = new List<(long Id, long JobId)>();
        await using (var sc = context.CreateSqlContainer())
        {
            sc.AppendQuery("SELECT ")
                .AppendName("Id").AppendComma()
                .AppendName("JobId")
                .AppendQuery(" FROM ").AppendQuery(WrappedTableName);
            if (lockMode == ClaimLockMode.SqlServer)
            {
                sc.AppendQuery(" WITH (UPDLOCK, READPAST, ROWLOCK)");
            }

            sc.AppendWhere();
            sc.AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
            sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");
            sc.AppendQuery(" ORDER BY ").AppendName("Id").AppendQuery(" ASC");
            if (lockMode == ClaimLockMode.PostgreSql)
            {
                sc.AppendQuery(" FOR UPDATE SKIP LOCKED");
            }

            context.Dialect.AppendPaging(sc.Query, 0, CandidateBatchSize);
            await using var reader = await sc.ExecuteReaderAsync(CommandType.Text, ct);
            while (await reader.ReadAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
                candidates.Add((reader.GetInt64(0), reader.GetInt64(1)));
            }
        }

        return candidates;
    }

    private async Task<bool> TryClaimAsync(long id, string queue, string fetchToken, CancellationToken ct, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        for (var attempt = 1; attempt <= MaxTransientClaimAttempts; attempt++)
        {
            try
            {
                await using var sc = ctx.CreateSqlContainer();
                sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
                sc.AppendName("FetchedAt").AppendEquals()
                    .AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, _utcNow()));
                sc.AppendComma().AppendName("FetchToken").AppendEquals()
                    .AppendParam(sc.AddParameterWithValue("fetchToken", DbType.String, fetchToken));
                sc.AppendWhere();
                sc.AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
                sc.AppendAnd().AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.Int64, id));
                sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");
                return await sc.ExecuteNonQueryAsync(CommandType.Text, ct) == 1;
            }
            catch (DatabaseException ex) when (IsTransientClaimConflict(ctx, ex) && attempt < MaxTransientClaimAttempts)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(5 * attempt), ct);
            }
        }

        return false;
    }

    private static bool IsTransientClaimConflict(IDatabaseContext context, DatabaseException exception)
    {
        if (exception.IsTransient == true)
        {
            return true;
        }

        return context.Product == SupportedDatabase.Firebird
            && exception.Message.Contains("deadlock", StringComparison.OrdinalIgnoreCase)
            && exception.Message.Contains("concurrent update", StringComparison.OrdinalIgnoreCase);
    }
}
