using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;
using pengdows.crud.enums;

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

        using var tx = Context.Product == pengdows.crud.enums.SupportedDatabase.PostgreSql 
            ? Context.BeginTransaction(System.Data.IsolationLevel.ReadCommitted, pengdows.crud.enums.ExecutionType.Write) 
            : Context.BeginTransaction(IsolationProfile.SafeNonBlockingReads);
            try
            {
                // Step 1: SELECT the lowest-Id unfetched candidate for this queue
                await using var selectSc = tx.CreateSqlContainer();
                selectSc.AppendQuery("SELECT ")
                    .AppendName("Id").AppendComma()
                    .AppendName("JobId").AppendComma()
                    .AppendName("Queue")
                    .AppendQuery(" FROM ").AppendQuery(WrappedTableName).AppendWhere();
                selectSc.AppendName("Queue").AppendEquals().AppendParam(selectSc.AddParameterWithValue("queue", DbType.String, queue));
                selectSc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");
                selectSc.AppendQuery(" ORDER BY ").AppendName("Id").AppendQuery(" ASC");
                Context.Dialect.AppendPaging(selectSc.Query, 0, 1);

                long candidateId;
                long candidateJobId;
                string candidateQueue;
                await using (var reader = await selectSc.ExecuteReaderAsync(CommandType.Text, ct))
                {
                    if (!await reader.ReadAsync(ct))
                    {
                        tx.Rollback();
                        continue;
                    }
                    candidateId    = reader.GetInt64(0);
                    candidateJobId = reader.GetInt64(1);
                    candidateQueue = reader.GetString(2);
                }

                // Step 2: Claim the row atomically — if another worker raced us the WHERE
                // FetchedAt IS NULL guard ensures 0 rows affected and we skip to next queue.
                await using var updateSc = tx.CreateSqlContainer();
                updateSc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
                updateSc.AppendName("FetchedAt").AppendEquals()
                    .AppendParam(updateSc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
                updateSc.AppendWhere();
                updateSc.AppendName("Queue").AppendEquals().AppendParam(updateSc.AddParameterWithValue("queue2", DbType.String, candidateQueue));
                updateSc.AppendAnd().AppendName("Id").AppendEquals().AppendParam(updateSc.AddParameterWithValue("id", DbType.Int64, candidateId));
                updateSc.AppendAnd().AppendName("FetchedAt").AppendQuery(" IS NULL");

                if (await updateSc.ExecuteNonQueryAsync(CommandType.Text, ct) == 0)
                {
                    tx.Rollback();
                    continue;
                }

                tx.Commit();
                return (candidateJobId, candidateQueue);
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        return null;
    }
}
