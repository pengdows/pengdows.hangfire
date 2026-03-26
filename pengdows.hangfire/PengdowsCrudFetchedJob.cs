namespace pengdows.hangfire;

using System;
using Hangfire.Storage;

public sealed class PengdowsCrudFetchedJob : IFetchedJob
{
    private readonly PengdowsCrudJobStorage _storage;
    private readonly long _jobId;
    private readonly string _jobIdString;
    private readonly string _queue;
    private bool _disposed;
    private bool _removedFromQueue;

    public PengdowsCrudFetchedJob(PengdowsCrudJobStorage storage, long jobId, string queue)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _jobId = jobId;
        _jobIdString = jobId.ToString();
        _queue = queue;
    }

    public string JobId => _jobIdString;

    public void RemoveFromQueue() => _removedFromQueue = true;

    public void Requeue() { }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_removedFromQueue)
        {
            _storage.JobQueues.AcknowledgeAsync(_jobId, _queue).GetAwaiter().GetResult();
        }
        else
        {
            _storage.JobQueues.RequeueAsync(_jobId, _queue).GetAwaiter().GetResult();
        }
    }
}
