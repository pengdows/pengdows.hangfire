namespace pengdows.hangfire;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Server;
using Hangfire.Storage;
using pengdows.hangfire.contracts;
using pengdows.hangfire.models;
using HangfireJob = Hangfire.Common.Job;
using ModelJob = pengdows.hangfire.models.Job;

public sealed class PengdowsCrudConnection : JobStorageConnection, IHangfireConnection
{
    private readonly PengdowsCrudJobStorage _storage;

    public PengdowsCrudConnection(PengdowsCrudJobStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public override IWriteOnlyTransaction CreateWriteTransaction()
        => new PengdowsCrudWriteOnlyTransaction(_storage);

    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        => new PengdowsCrudDistributedLock(_storage, resource, timeout);

    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
    {
        if (queues == null || queues.Length == 0)
        {
            throw new ArgumentNullException(nameof(queues));
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _storage.JobQueues.FetchNextJobAsync(queues, cancellationToken).GetAwaiter().GetResult();
            if (result.HasValue)
            {
                return new PengdowsCrudFetchedJob(_storage, result.Value.JobId, result.Value.Queue);
            }

            cancellationToken.WaitHandle.WaitOne(_storage.Options.QueuePollInterval);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return null!;
    }

    public override string CreateExpiredJob(HangfireJob job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        if (job == null)
        {
            throw new ArgumentNullException(nameof(job));
        }

        if (parameters == null)
        {
            throw new ArgumentNullException(nameof(parameters));
        }

        var hfJob = new ModelJob
        {
            InvocationData = JsonHelper.Serialize(InvocationData.SerializeJob(job)),
            Arguments = JsonHelper.Serialize(job.Args),
            CreatedAt = createdAt,
            ExpireAt = createdAt.Add(expireIn)
        };

        _storage.Jobs.CreateAsync(hfJob).GetAwaiter().GetResult();

        var paramTasks = parameters
            .Select(p => _storage.JobParameters.UpsertAsync(new JobParameter { JobID = hfJob.ID, Name = p.Key, Value = p.Value }))
            .ToArray();
        foreach (var pt in paramTasks) pt.AsTask().Wait();

        return hfJob.ID.ToString();
    }

    public override void SetJobParameter(string jobId, string name, string value)
    {
        if (jobId == null)
        {
            throw new ArgumentNullException(nameof(jobId));
        }

        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _storage.JobParameters.UpsertAsync(new JobParameter { JobID = id, Name = name, Value = value })
            .GetAwaiter().GetResult();
    }

    public override string GetJobParameter(string jobId, string name)
    {
        if (jobId == null)
        {
            throw new ArgumentNullException(nameof(jobId));
        }

        if (!long.TryParse(jobId, out var id))
        {
            return null!;
        }

        var param = _storage.JobParameters.RetrieveOneAsync(new JobParameter { JobID = id, Name = name })
            .GetAwaiter().GetResult();
        return param?.Value!;
    }

    public override JobData GetJobData(string jobId)
    {
        if (jobId == null)
        {
            throw new ArgumentNullException(nameof(jobId));
        }

        if (!long.TryParse(jobId, out var id))
        {
            return null!;
        }

        var job = _storage.Jobs.RetrieveOneAsync(id).GetAwaiter().GetResult();
        if (job == null)
        {
            return null!;
        }

        var invocationData = JsonHelper.Deserialize<InvocationData>(job.InvocationData);
        return new JobData
        {
            InvocationData = invocationData,
            CreatedAt = job.CreatedAt,
            State = job.StateName
        };
    }

    public override StateData GetStateData(string jobId)
    {
        if (jobId == null)
        {
            throw new ArgumentNullException(nameof(jobId));
        }

        if (!long.TryParse(jobId, out var id))
        {
            return null!;
        }

        var state = _storage.JobStates.GetLatestAsync(id).GetAwaiter().GetResult();
        if (state == null)
        {
            return null!;
        }

        return new StateData
        {
            Name   = state.Name!,
            Reason = state.Reason,
            Data   = state.Data == null
                ? new Dictionary<string, string>()
                : JsonHelper.Deserialize<Dictionary<string, string>>(state.Data)
        };
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (keyValuePairs == null)
        {
            throw new ArgumentNullException(nameof(keyValuePairs));
        }

        var tasks = keyValuePairs
            .Select(pair => _storage.Hashes.UpsertAsync(new Hash { Key = key, Field = pair.Key, Value = pair.Value }))
            .ToArray();
        foreach (var t in tasks) t.AsTask().Wait();
    }

    public override Dictionary<string, string> GetAllEntriesFromHash(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Hashes.GetAllEntriesAsync(key).GetAwaiter().GetResult();
    }

    public override void AnnounceServer(string serverId, ServerContext context)
    {
        if (serverId == null)
        {
            throw new ArgumentNullException(nameof(serverId));
        }

        if (context == null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        var now = DateTime.UtcNow;
        _storage.Servers.UpsertAsync(new Server
        {
            ID = serverId,
            Data = JsonHelper.Serialize(new ServerData { Queues = context.Queues, WorkerCount = context.WorkerCount, StartedAt = now }),
            LastHeartbeat = now
        }).GetAwaiter().GetResult();
    }

    public override void RemoveServer(string serverId)
    {
        if (serverId == null)
        {
            throw new ArgumentNullException(nameof(serverId));
        }

        _storage.Servers.DeleteAsync(serverId).GetAwaiter().GetResult();
    }

    public override void Heartbeat(string serverId)
    {
        if (serverId == null)
        {
            throw new ArgumentNullException(nameof(serverId));
        }

        _storage.Servers.UpdateHeartbeatAsync(serverId).GetAwaiter().GetResult();
    }

    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
        var cutoff = DateTime.UtcNow.Subtract(timeOut);
        return _storage.Servers.RemoveTimedOutAsync(cutoff).GetAwaiter().GetResult();
    }

    public override HashSet<string> GetAllItemsFromSet(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetAllItemsAsync(key).GetAwaiter().GetResult();
    }

    public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetFirstByLowestScoreAsync(key, fromScore, toScore).GetAwaiter().GetResult()!;
    }

    public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetFirstByLowestScoreAsync(key, fromScore, toScore, count).GetAwaiter().GetResult();
    }

    public override long GetSetCount(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetCountAsync(key).GetAwaiter().GetResult();
    }

    public override long GetSetCount(IEnumerable<string> keys, int limit)
    {
        if (keys == null)
        {
            throw new ArgumentNullException(nameof(keys));
        }

        var tasks = keys.Select(k => _storage.Sets.GetCountAsync(k)).ToList();
        Task.WaitAll(tasks.ToArray());
        return Math.Min(tasks.Sum(t => t.Result), limit);
    }

    public override bool GetSetContains(string key, string value)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (value == null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        return _storage.Sets.ContainsAsync(key, value).GetAwaiter().GetResult();
    }

    public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetRangeAsync(key, startingFrom, endingAt).GetAwaiter().GetResult();
    }

    public override TimeSpan GetSetTtl(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Sets.GetTtlAsync(key).GetAwaiter().GetResult();
    }

    public override string GetValueFromHash(string key, string name)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (name == null)
        {
            throw new ArgumentNullException(nameof(name));
        }

        return _storage.Hashes.GetValueAsync(key, name).GetAwaiter().GetResult()!;
    }

    public override long GetHashCount(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Hashes.GetCountAsync(key).GetAwaiter().GetResult();
    }

    public override TimeSpan GetHashTtl(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Hashes.GetTtlAsync(key).GetAwaiter().GetResult();
    }

    public override long GetListCount(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Lists.GetCountAsync(key).GetAwaiter().GetResult();
    }

    public override TimeSpan GetListTtl(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Lists.GetTtlAsync(key).GetAwaiter().GetResult();
    }

    public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Lists.GetRangeAsync(key, startingFrom, endingAt).GetAwaiter().GetResult();
    }

    public override List<string> GetAllItemsFromList(string key)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _storage.Lists.GetAllAsync(key).GetAwaiter().GetResult();
    }

    public override DateTime GetUtcDateTime() => DateTime.UtcNow;
}
