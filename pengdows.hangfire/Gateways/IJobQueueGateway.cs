using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;


namespace pengdows.hangfire.gateways;

public interface IJobQueueGateway : ITableGateway<JobQueue, long>
{
    Task<(long JobId, string Queue)?> FetchNextJobAsync(string[] queues, CancellationToken ct);
    Task<List<string>> GetDistinctQueuesAsync();
    Task<int> AcknowledgeAsync(long jobId, string queue);
    Task<int> RequeueAsync(long jobId, string queue);
    Task<int> RequeueStaleAsync(DateTime cutoff);
    Task<List<JobQueue>> GetPagedByQueueAsync(string queue, int from, int count, bool fetched);
}
