namespace pengdows.hangfire;

using System;
using Hangfire.Storage;
using System.Threading;
using System.Threading.Tasks;

public sealed class PengdowsCrudFetchedJob : IFetchedJob
{
    private readonly PengdowsCrudJobStorage _storage;
    private readonly long _jobId;
    private readonly string _jobIdString;
    private readonly string _queue;
    private readonly string? _fetchToken;
    private readonly Timer? _keepAlive;
    private bool _disposed;
    private bool _removedFromQueue;
    private bool _requeued;

    public PengdowsCrudFetchedJob(PengdowsCrudJobStorage storage, long jobId, string queue, string? fetchToken = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _jobId = jobId;
        _jobIdString = jobId.ToString();
        _queue = queue;
        _fetchToken = fetchToken;
        if (fetchToken != null)
        {
            var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks,
                _storage.Options.InvisibilityTimeout.Ticks / 3));
            _keepAlive = new Timer(_ => _ = KeepAliveAsync(), null, interval, Timeout.InfiniteTimeSpan);
        }
    }

    public string JobId => _jobIdString;

    public void RemoveFromQueue() => _removedFromQueue = true;

    public void Requeue()
    {
        if (_disposed || _requeued)
        {
            return;
        }

        RequeueCurrentClaim();
        _requeued = true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _keepAlive?.Dispose();

        if (_removedFromQueue)
        {
            AcknowledgeCurrentClaim();
        }
        else if (!_requeued)
        {
            RequeueCurrentClaim();
        }
    }

    private void AcknowledgeCurrentClaim()
    {
        if (_fetchToken == null)
            _storage.JobQueues.AcknowledgeAsync(_jobId, _queue).GetAwaiter().GetResult();
        else
            _storage.JobQueues.AcknowledgeAsync(_jobId, _queue, _fetchToken).GetAwaiter().GetResult();
    }

    private void RequeueCurrentClaim()
    {
        if (_fetchToken == null)
            _storage.JobQueues.RequeueAsync(_jobId, _queue).GetAwaiter().GetResult();
        else
            _storage.JobQueues.RequeueAsync(_jobId, _queue, _fetchToken).GetAwaiter().GetResult();
    }

    private async Task KeepAliveAsync()
    {
        if (_disposed || _fetchToken == null)
            return;

        try
        {
            var updated = await _storage.JobQueues.KeepAliveAsync(_jobId, _queue, _fetchToken);
            if (updated == 0)
                _keepAlive?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }
        catch (Exception)
        {
            // A failed heartbeat must not become an unobserved fire-and-forget exception.
            // The watchdog remains the recovery path if the claim cannot be renewed.
        }

        if (!_disposed)
        {
            try { _keepAlive?.Change(_storage.Options.InvisibilityTimeout / 3, Timeout.InfiniteTimeSpan); }
            catch (ObjectDisposedException) { }
        }
    }
}
