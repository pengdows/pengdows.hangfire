namespace pengdows.hangfire;

using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using pengdows.hangfire.gateways;
using Hangfire.Storage;

public sealed class PengdowsCrudDistributedLock : IDisposable
{
    private static readonly ILog Logger = LogProvider.For<PengdowsCrudDistributedLock>();

    private readonly IDistributedLockGateway _gateway;
    private readonly string _resource;
    private readonly string _ownerId;
    private readonly TimeSpan _ttl;
    private readonly TimeSpan _heartbeatInterval;
    private readonly Timer _heartbeat;
    private int _version;
    private int _disposed;
    private volatile bool _leaseLost;
    private int _consecutiveRenewalFailures;

    public bool LeaseLost => _leaseLost;

    public PengdowsCrudDistributedLock(PengdowsCrudJobStorage storage, string resource, TimeSpan timeout)
    {
        if (storage == null) throw new ArgumentNullException(nameof(storage));
        if (resource == null) throw new ArgumentNullException(nameof(resource));

        _gateway           = storage.Locks;
        _resource          = resource;
        _ttl               = storage.Options.DistributedLockTtl;
        _heartbeatInterval = TimeSpan.FromTicks(_ttl.Ticks / 5);

        var (ownerId, version) = AcquireAsync(storage, resource, timeout).GetAwaiter().GetResult();
        _ownerId = ownerId;
        _version = version;

        _heartbeat = new Timer(_ => { _ = RenewAsync(); }, null, _heartbeatInterval, Timeout.InfiniteTimeSpan);
    }

    private static async Task<(string ownerId, int version)> AcquireAsync(
        PengdowsCrudJobStorage storage, string resource, TimeSpan timeout)
    {
        var ownerId    = Guid.NewGuid().ToString("N");
        var deadline   = DateTime.UtcNow + timeout;
        var retryDelay = storage.Options.DistributedLockRetryDelay;
        var jitter     = storage.Options.DistributedLockRetryJitter;

        while (true)
        {
            var now     = DateTime.UtcNow;
            var claimed = await storage.Locks.TryAcquireAsync(resource, ownerId, now + storage.Options.DistributedLockTtl, now);
            if (claimed)
            {
                return (ownerId, 1);
            }

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                throw new DistributedLockTimeoutException(resource);
            }

            var sleep = jitter ? JitteredDelay(retryDelay) : retryDelay;
            if (sleep > remaining)
            {
                sleep = remaining;
            }
            await Task.Delay(sleep);
        }
    }

    /// <summary>
    /// Returns a delay sampled uniformly from [base/2, base*3/2].
    /// Breaks phase alignment: waiters that all failed at the same instant no longer
    /// retry simultaneously and collide on the same release edge.
    /// </summary>
    private static TimeSpan JitteredDelay(TimeSpan baseDelay)
    {
        var baseMs = (long)baseDelay.TotalMilliseconds;
        var ms     = baseMs / 2 + Random.Shared.NextInt64(baseMs);
        return TimeSpan.FromMilliseconds(ms);
    }

    private async Task RenewAsync()
    {
        try
        {
            var renewed = await _gateway.TryRenewAsync(
                _resource, _ownerId, _version, DateTime.UtcNow + _ttl);

            if (!renewed)
            {
                _leaseLost = true;
                Logger.WarnFormat("Distributed lock '{0}' lease lost — another worker may have taken it.", _resource);
                return;
            }

            _consecutiveRenewalFailures = 0;
            _version++;
        }
        catch (Exception ex)
        {
            _consecutiveRenewalFailures++;
            if (_consecutiveRenewalFailures == 1)
            {
                Logger.DebugException($"Transient error renewing lock '{_resource}'.", ex);
            }
            else if (_consecutiveRenewalFailures % 3 == 0)
            {
                Logger.WarnException(
                    $"Repeated transient errors renewing lock '{_resource}' ({_consecutiveRenewalFailures} consecutive).", ex);
            }
        }

        if (_disposed == 0)
        {
            _heartbeat.Change(_heartbeatInterval, Timeout.InfiniteTimeSpan);
        }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
        {
            return;
        }

        _heartbeat.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _heartbeat.Dispose();

        try
        {
            _gateway.ReleaseAsync(_resource, _ownerId).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Logger.WarnException(
                $"Failed to release distributed lock '{_resource}' — row may remain until TTL expiry.", ex);
        }
    }
}
