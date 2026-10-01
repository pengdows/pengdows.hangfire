using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class JobQueueGateway : TableGateway<JobQueue, long>, IJobQueueGateway
{
    private readonly Func<DateTime> _utcNow;
    public JobQueueGateway(IDatabaseContext context, Func<DateTime>? utcNow = null) : base(context)
        => _utcNow = utcNow ?? (() => DateTime.UtcNow);

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
        foreach (var queue in queues)
        {
            ct.ThrowIfCancellationRequested();

            // Stream candidates lazily — no LIMIT, no allocation into a list.
            // The FetchedAt IS NULL guard in TryClaimAsync is the correctness gate.
            await using var sc = ctx.CreateSqlContainer();
            sc.AppendQuery("SELECT ")
                .AppendName("Id").AppendComma()
                .AppendName("JobId")
                .AppendQuery(" FROM ").AppendQuery(WrappedTableName).AppendWhere();
            sc.AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
            sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");
            sc.AppendQuery(" ORDER BY ").AppendName("Id").AppendQuery(" ASC");

            await using var reader = await sc.ExecuteReaderAsync(CommandType.Text, ct);
            while (await reader.ReadAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
                var id    = reader.GetInt64(0);
                var jobId = reader.GetInt64(1);
                var fetchToken = Guid.NewGuid().ToString("N");
                if (await TryClaimAsync(id, queue, fetchToken, ct, ctx))
                {
                    return (jobId, queue, fetchToken);
                }
            }
        }

        return null;
    }

    private async Task<bool> TryClaimAsync(long id, string queue, string fetchToken, CancellationToken ct, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
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
}
