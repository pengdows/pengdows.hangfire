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
    public JobQueueGateway(IDatabaseContext context) : base(context) { }

    public async Task<int> AcknowledgeAsync(long jobId, string queue)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendAnd().AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> RequeueAsync(long jobId, string queue)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendQuery(" = NULL WHERE ");
        sc.AppendName("JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendAnd().AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> RequeueStaleAsync(DateTime cutoff)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendQuery(" = NULL");
        sc.AppendWhere();
        sc.AppendName("FetchedAt").AppendQuery(" IS NOT NULL");
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" <= ")
          .AppendParam(sc.AddParameterWithValue("cutoff", DbType.DateTime, cutoff));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<List<string>> GetDistinctQueuesAsync()
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT DISTINCT ").AppendName("Queue").AppendQuery(" FROM ").AppendQuery(WrappedTableName);
        await using var reader = await sc.ExecuteReaderAsync();
        var queues = new List<string>();
        while (await reader.ReadAsync())
        {
            queues.Add(reader.GetString(0));
        }
        return queues;
    }

    public async Task<List<JobQueue>> GetPagedByQueueAsync(string queue, int from, int count, bool fetched)
    {
        var sc = BuildBaseRetrieve("q");
        sc.AppendWhere();
        sc.AppendName("q.Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("q.FetchedAt").AppendQuery(fetched ? " IS NOT NULL" : " IS NULL");
        sc.AppendQuery(" ORDER BY ").AppendName("q.Id").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, from, count);
        return await LoadListAsync(sc);
    }

    public async Task<(long JobId, string Queue)?> FetchNextJobAsync(string[] queues, CancellationToken ct)
    {
        foreach (var queue in queues)
        {
            ct.ThrowIfCancellationRequested();

            // Stream candidates lazily — no LIMIT, no allocation into a list.
            // The FetchedAt IS NULL guard in TryClaimAsync is the correctness gate.
            await using var sc = Context.CreateSqlContainer();
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
                if (await TryClaimAsync(id, queue, ct))
                {
                    return (jobId, queue);
                }
            }
        }

        return null;
    }

    private async Task<bool> TryClaimAsync(long id, string queue, CancellationToken ct)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("FetchedAt").AppendEquals()
            .AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendWhere();
        sc.AppendName("Queue").AppendEquals().AppendParam(sc.AddParameterWithValue("queue", DbType.String, queue));
        sc.AppendAnd().AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.Int64, id));
        sc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");
        return await sc.ExecuteNonQueryAsync(CommandType.Text, ct) == 1;
    }
}
