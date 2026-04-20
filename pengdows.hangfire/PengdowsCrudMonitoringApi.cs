namespace pengdows.hangfire;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using pengdows.hangfire.contracts;
using pengdows.crud.metrics;
using ModelJob = pengdows.hangfire.models.Job;

public sealed class PengdowsCrudMonitoringApi : IHangfireMonitor
{
    private readonly PengdowsCrudJobStorage _storage;

    public PengdowsCrudMonitoringApi(PengdowsCrudJobStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public IDictionary<string, DatabaseMetrics> GetDatabaseMetrics()
    {
        var metrics = new Dictionary<string, DatabaseMetrics>();
        
        // Main storage database
        var db = _storage.DatabaseContext;
        metrics[db.Name ?? "Hangfire"] = db.Metrics;

        // Additional registered contexts
        foreach (var ctx in _storage.Options.AdditionalMetricsContexts)
        {
            if (ctx == null) continue;
            var name = ctx.Name ?? ctx.GetType().Name;
            metrics[name] = ctx.Metrics;
        }

        return metrics;
    }

    public string GetDatabaseMetricGrid()
    {
        var sb = new StringBuilder();
        
        // Main storage database
        var db = _storage.DatabaseContext;
        sb.AppendLine(db.Metrics.ToMetricGrid(db.Name ?? "Hangfire", db.ConnectionMode.ToString()));
        sb.AppendLine();

        // Additional registered contexts
        foreach (var ctx in _storage.Options.AdditionalMetricsContexts)
        {
            if (ctx == null) continue;
            var name = ctx.Name ?? ctx.GetType().Name;
            sb.AppendLine(ctx.Metrics.ToMetricGrid(name, ctx.ConnectionMode.ToString()));
            sb.AppendLine();
        }

        return sb.ToString();
    }

    public StatisticsDto GetStatistics()
    {
        var servers    = _storage.Servers.CountAllAsync().AsTask();
        var recurring  = _storage.Hashes.CountWhereAsync("Key", "recurring-job:%", isLike: true).AsTask();
        var enqueued   = _storage.JobQueues.CountWhereNullAsync("FetchedAt").AsTask();
        var processing = CountByState(ProcessingState.StateName);
        var scheduled  = CountByState(ScheduledState.StateName);
        var failed     = CountByState(FailedState.StateName);
        var succeeded  = _storage.AggregatedCounters.GetValueAsync("stats:succeeded");
        var deleted    = _storage.AggregatedCounters.GetValueAsync("stats:deleted");

        Task.WaitAll(servers, recurring, enqueued, processing, scheduled, failed, succeeded, deleted);

        return new StatisticsDto
        {
            Servers    = servers.Result,
            Recurring  = recurring.Result,
            Enqueued   = enqueued.Result,
            Processing = processing.Result,
            Scheduled  = scheduled.Result,
            Failed     = failed.Result,
            Succeeded  = succeeded.Result,
            Deleted    = deleted.Result,
        };
    }

    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        var queues = _storage.JobQueues.GetDistinctQueuesAsync().GetAwaiter().GetResult();

        var lengthTasks  = queues.Select(q => _storage.JobQueues.CountWhereEqualsAsync("Queue", q, andWhereNull:    "FetchedAt").AsTask()).ToArray();
        var fetchedTasks = queues.Select(q => _storage.JobQueues.CountWhereEqualsAsync("Queue", q, andWhereNotNull: "FetchedAt").AsTask()).ToArray();

        Task.WaitAll(lengthTasks.Concat(fetchedTasks).ToArray());

        return queues.Select((q, i) => new QueueWithTopEnqueuedJobsDto
        {
            Name      = q,
            Length    = lengthTasks[i].Result,
            Fetched   = (int)fetchedTasks[i].Result,
            FirstJobs = EnqueuedJobs(q, 0, 5)
        }).ToList<QueueWithTopEnqueuedJobsDto>();
    }

    public IList<ServerDto> Servers()
    {
        var sc = _storage.Servers.BuildBaseRetrieve("s");
        var list = _storage.Servers.LoadListAsync(sc).GetAwaiter().GetResult();
        return list.Select(s =>
        {
            var data = s.Data != null
                ? JsonHelper.Deserialize<ServerData>(s.Data)
                : null;
            return new ServerDto
            {
                Name         = s.ID,
                Heartbeat    = s.LastHeartbeat,
                StartedAt    = data?.StartedAt ?? s.LastHeartbeat,
                WorkersCount = data?.WorkerCount ?? 0,
                Queues       = data?.Queues ?? Array.Empty<string>()
            };
        }).ToList();
    }

    public JobDetailsDto JobDetails(string jobId)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return null!;
        }

        var job = _storage.Jobs.RetrieveOneAsync(id).GetAwaiter().GetResult();
        if (job == null)
        {
            return null!;
        }

        var statesTask     = _storage.JobStates.GetAllForJobAsync(id);
        var parametersTask = _storage.JobParameters.GetAllForJobAsync(id);
        Task.WaitAll(statesTask, parametersTask);

        var invocationData = JsonHelper.Deserialize<InvocationData>(job.InvocationData);
        Hangfire.Common.Job? hfJob = null;
        JobLoadException? loadException = null;
        try { hfJob = invocationData.DeserializeJob(); }
        catch (JobLoadException ex) { loadException = ex; }

        return new JobDetailsDto
        {
            Job            = hfJob,
            LoadException  = loadException,
            InvocationData = invocationData,
            CreatedAt      = job.CreatedAt,
            ExpireAt       = job.ExpireAt,
            Properties     = parametersTask.Result,
            History        = statesTask.Result.Select(s => new StateHistoryDto
            {
                StateName = s.Name!,
                Reason    = s.Reason,
                CreatedAt = s.CreatedAt,
                Data      = s.Data != null
                    ? JsonHelper.Deserialize<Dictionary<string, string>>(s.Data)
                    : new Dictionary<string, string>()
            }).ToList()
        };
    }

    public long EnqueuedCount(string queue) =>
        _storage.JobQueues.CountWhereEqualsAsync("Queue", queue, andWhereNull: "FetchedAt").GetAwaiter().GetResult();

    public long FetchedCount(string queue) =>
        _storage.JobQueues.CountWhereEqualsAsync("Queue", queue, andWhereNotNull: "FetchedAt").GetAwaiter().GetResult();

    public long ScheduledCount()      => CountByState(ScheduledState.StateName).GetAwaiter().GetResult();
    public long FailedCount()         => CountByState(FailedState.StateName).GetAwaiter().GetResult();
    public long ProcessingCount()     => CountByState(ProcessingState.StateName).GetAwaiter().GetResult();
    public long SucceededListCount()  => CountByState(SucceededState.StateName).GetAwaiter().GetResult();
    public long DeletedListCount()    => CountByState(DeletedState.StateName).GetAwaiter().GetResult();

    public IDictionary<DateTime, long> SucceededByDatesCount() => GetTimeline("stats:succeeded", 7,  i => DateTime.UtcNow.Date.AddDays(-i),  d => d.ToString("yyyy-MM-dd"));
    public IDictionary<DateTime, long> FailedByDatesCount()    => GetTimeline("stats:failed",    7,  i => DateTime.UtcNow.Date.AddDays(-i),  d => d.ToString("yyyy-MM-dd"));
    public IDictionary<DateTime, long> HourlySucceededJobs()   => GetTimeline("stats:succeeded", 24, i => TruncateToHour(DateTime.UtcNow.AddHours(-i)), d => d.ToString("yyyy-MM-dd-HH"));
    public IDictionary<DateTime, long> HourlyFailedJobs()      => GetTimeline("stats:failed",    24, i => TruncateToHour(DateTime.UtcNow.AddHours(-i)), d => d.ToString("yyyy-MM-dd-HH"));

    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(ProcessingState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<ProcessingJobDto>(jobs.Select(j =>
        {
            var (data, _) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            return KeyValuePair.Create(j.ID.ToString(), new ProcessingJobDto
            {
                Job               = hfJob,
                LoadException     = ex,
                InvocationData    = inv,
                InProcessingState = j.StateName == ProcessingState.StateName,
                ServerId          = data.GetValueOrDefault("ServerId"),
                StartedAt         = TryParseDateTime(data.GetValueOrDefault("StartedAt")),
                StateData         = data
            });
        }));
    }

    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(ScheduledState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<ScheduledJobDto>(jobs.Select(j =>
        {
            var (data, _) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            return KeyValuePair.Create(j.ID.ToString(), new ScheduledJobDto
            {
                Job              = hfJob,
                LoadException    = ex,
                InvocationData   = inv,
                InScheduledState = j.StateName == ScheduledState.StateName,
                EnqueueAt        = TryParseDateTime(data.GetValueOrDefault("EnqueueAt")) ?? DateTime.MinValue,
                ScheduledAt      = TryParseDateTime(data.GetValueOrDefault("ScheduledAt")),
                StateData        = data
            });
        }));
    }

    public JobList<SucceededJobDto> SucceededJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(SucceededState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<SucceededJobDto>(jobs.Select(j =>
        {
            var (data, _) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            long.TryParse(data.GetValueOrDefault("PerformanceDuration"), out var dur);
            long.TryParse(data.GetValueOrDefault("Latency"), out var lat);
            return KeyValuePair.Create(j.ID.ToString(), new SucceededJobDto
            {
                Job              = hfJob,
                LoadException    = ex,
                InvocationData   = inv,
                InSucceededState = j.StateName == SucceededState.StateName,
                Result           = data.GetValueOrDefault("Result"),
                TotalDuration    = dur + lat,
                SucceededAt      = TryParseDateTime(data.GetValueOrDefault("SucceededAt")),
                StateData        = data
            });
        }));
    }

    public JobList<FailedJobDto> FailedJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(FailedState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<FailedJobDto>(jobs.Select(j =>
        {
            var (data, reason) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            return KeyValuePair.Create(j.ID.ToString(), new FailedJobDto
            {
                Job              = hfJob,
                LoadException    = ex,
                InvocationData   = inv,
                InFailedState    = j.StateName == FailedState.StateName,
                Reason           = reason,
                FailedAt         = TryParseDateTime(data.GetValueOrDefault("FailedAt")),
                ExceptionType    = data.GetValueOrDefault("ExceptionType"),
                ExceptionMessage = data.GetValueOrDefault("ExceptionMessage"),
                ExceptionDetails = data.GetValueOrDefault("ExceptionDetails"),
                StateData        = data
            });
        }));
    }

    public JobList<DeletedJobDto> DeletedJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(DeletedState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<DeletedJobDto>(jobs.Select(j =>
        {
            var (data, _) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            return KeyValuePair.Create(j.ID.ToString(), new DeletedJobDto
            {
                Job            = hfJob,
                LoadException  = ex,
                InvocationData = inv,
                InDeletedState = j.StateName == DeletedState.StateName,
                DeletedAt      = TryParseDateTime(data.GetValueOrDefault("DeletedAt")),
                StateData      = data
            });
        }));
    }

    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        var queueItems = _storage.JobQueues.GetPagedByQueueAsync(queue, from, perPage, fetched: false).GetAwaiter().GetResult();
        if (queueItems.Count == 0)
        {
            return new JobList<EnqueuedJobDto>(Enumerable.Empty<KeyValuePair<string, EnqueuedJobDto>>());
        }

        var jobIds = queueItems.Select(qi => qi.JobID).ToList();
        var jobs = _storage.Jobs.RetrieveAsync(jobIds).GetAwaiter().GetResult();
        var jobMap = jobs.ToDictionary(j => j.ID);
        var states = LoadStateBatch(jobs);
        return new JobList<EnqueuedJobDto>(queueItems
            .Where(qi => jobMap.ContainsKey(qi.JobID))
            .Select(qi =>
            {
                var job = jobMap[qi.JobID];
                var (data, _) = states[job.ID];
                var (inv, hfJob, ex) = DeserializeJob(job);
                return KeyValuePair.Create(job.ID.ToString(), new EnqueuedJobDto
                {
                    Job             = hfJob,
                    LoadException   = ex,
                    InvocationData  = inv,
                    State           = job.StateName,
                    InEnqueuedState = job.StateName == EnqueuedState.StateName,
                    EnqueuedAt      = TryParseDateTime(data.GetValueOrDefault("EnqueuedAt")),
                    StateData       = data
                });
            }));
    }

    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        var queueItems = _storage.JobQueues.GetPagedByQueueAsync(queue, from, perPage, fetched: true).GetAwaiter().GetResult();
        if (queueItems.Count == 0)
        {
            return new JobList<FetchedJobDto>(Enumerable.Empty<KeyValuePair<string, FetchedJobDto>>());
        }

        var jobIds = queueItems.Select(qi => qi.JobID).ToList();
        var jobs = _storage.Jobs.RetrieveAsync(jobIds).GetAwaiter().GetResult();
        var jobMap = jobs.ToDictionary(j => j.ID);
        return new JobList<FetchedJobDto>(queueItems
            .Where(qi => jobMap.ContainsKey(qi.JobID))
            .Select(qi =>
            {
                var job = jobMap[qi.JobID];
                var (inv, hfJob, ex) = DeserializeJob(job);
                return KeyValuePair.Create(job.ID.ToString(), new FetchedJobDto
                {
                    Job            = hfJob,
                    LoadException  = ex,
                    InvocationData = inv,
                    State          = job.StateName,
                    FetchedAt      = qi.FetchedAt
                });
            }));
    }

    public JobList<EnqueuedJobDto> AwaitingJobs(int from, int count)
    {
        var jobs = _storage.Jobs.GetPagedByStateAsync(AwaitingState.StateName, from, count).GetAwaiter().GetResult();
        var states = LoadStateBatch(jobs);
        return new JobList<EnqueuedJobDto>(jobs.Select(j =>
        {
            var (data, _) = states[j.ID];
            var (inv, hfJob, ex) = DeserializeJob(j);
            return KeyValuePair.Create(j.ID.ToString(), new EnqueuedJobDto
            {
                Job             = hfJob,
                LoadException   = ex,
                InvocationData  = inv,
                State           = j.StateName,
                InEnqueuedState = false,
                StateData       = data
            });
        }));
    }

    public long AwaitingCount() => CountByState(AwaitingState.StateName).GetAwaiter().GetResult();

    private Task<long> CountByState(string stateName) =>
        _storage.Jobs.CountWhereAsync("StateName", stateName).AsTask();

    private IDictionary<DateTime, long> GetTimeline(string keyPrefix, int slots, Func<int, DateTime> slotAt, Func<DateTime, string> keySuffix)
    {
        var dates = Enumerable.Range(0, slots).Select(slotAt).ToArray();
        var keys  = dates.Select(d => $"{keyPrefix}:{keySuffix(d)}").ToArray();
        var counts = _storage.AggregatedCounters.GetTimelineAsync(keys).GetAwaiter().GetResult();
        return dates.Zip(keys, (d, k) => (d, k)).ToDictionary(x => x.d, x => counts.TryGetValue(x.k, out var v) ? v : 0L);
    }

    /// <summary>Loads the latest state for each job in parallel. Faulted individual lookups
    /// yield an empty data dictionary and null reason rather than failing the whole page.</summary>
    private Dictionary<long, (Dictionary<string, string> Data, string? Reason)> LoadStateBatch(List<ModelJob> jobs)
    {
        var tasks = jobs.ToDictionary(j => j.ID, j => _storage.JobStates.GetLatestAsync(j.ID));
        try
        {
            Task.WaitAll(tasks.Values.ToArray());
        }
        catch (AggregateException)
        {
            // Individual faulted tasks are handled per-key below.
        }

        return tasks.ToDictionary(kv => kv.Key, kv =>
        {
            if (kv.Value.IsFaulted || kv.Value.IsCanceled)
            {
                return (new Dictionary<string, string>(), (string?)null);
            }

            var state = kv.Value.Result;
            var data  = state?.Data != null
                ? JsonHelper.Deserialize<Dictionary<string, string>>(state.Data)
                : new Dictionary<string, string>();
            return (data, state?.Reason);
        });
    }

    private static (InvocationData inv, Hangfire.Common.Job? job, JobLoadException? ex) DeserializeJob(ModelJob modelJob)
    {
        var inv = JsonHelper.Deserialize<InvocationData>(modelJob.InvocationData);
        try
        {
            return (inv, inv.DeserializeJob(), null);
        }
        catch (JobLoadException ex)
        {
            return (inv, null, ex);
        }
    }

    private static DateTime? TryParseDateTime(string? value)
    {
        if (value == null)
        {
            return null;
        }

        return DateTime.TryParse(value, null, System.Globalization.DateTimeStyles.RoundtripKind, out var dt) ? dt : null;
    }

    private static DateTime TruncateToHour(DateTime dt) =>
        new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, DateTimeKind.Utc);
}
