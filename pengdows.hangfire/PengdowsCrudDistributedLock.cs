namespace pengdows.hangfire;

using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using pengdows.hangfire.gateways;
using Hangfire.Storage;

/// <summary>
/// Records which code path produced a successful lock acquisition.
/// Exposed via <see cref="PengdowsCrudDistributedLock.HowAcquired"/> for
/// test observability only — not a supported public contract.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum AcquireMode
{
    /// <summary>INSERT succeeded on the first attempt — no contention.</summary>
    InsertWin,

    /// <summary>
    /// INSERT eventually won after one or more 100 ms sleeps, meaning the
    /// previous holder released normally and the row disappeared before TTL.
    /// </summary>
    FollowRelease,

    /// <summary>
    /// <see cref="IDistributedLockGateway.TryDeleteExpiredAsync"/> deleted a
    /// stale row whose TTL had passed; the subsequent INSERT then won.
    /// The tail latency here is bounded by the previous holder's TTL.
    /// </summary>
    TtlSteal,
}

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

    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool LeaseLost => _leaseLost;

    /// <summary>Records which code path produced this acquisition.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public AcquireMode HowAcquired { get; private set; }

    /// <summary>
    /// Number of 100 ms (or configured) sleeps taken before the lock was
    /// acquired.  Zero for <see cref="AcquireMode.InsertWin"/>.
    /// For test observability only.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int AcquireRetryCount { get; private set; }

    /// <summary>
    /// Total wall-clock time spent sleeping in the retry loop, in milliseconds.
    /// Equals <see cref="AcquireRetryCount"/> × configured retry delay for
    /// <see cref="AcquireMode.FollowRelease"/>; may differ if the delay is
    /// jittered in future implementations.
    /// For test observability only.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public long AcquireSleepMs { get; private set; }

    public PengdowsCrudDistributedLock(PengdowsCrudJobStorage storage, string resource, TimeSpan timeout)
    {
        if (storage == null) throw new ArgumentNullException(nameof(storage));
        if (resource == null) throw new ArgumentNullException(nameof(resource));

        _gateway          = storage.Locks;
        _resource         = resource;
        _ttl              = storage.Options.DistributedLockTtl;
        _heartbeatInterval = TimeSpan.FromTicks(_ttl.Ticks / 5);

        var (ownerId, version, mode, retryCount, sleepMs) =
            AcquireAsync(storage, resource, timeout).GetAwaiter().GetResult();
        _ownerId         = ownerId;
        _version         = version;
        HowAcquired      = mode;
        AcquireRetryCount = retryCount;
        AcquireSleepMs   = sleepMs;

        _heartbeat = new Timer(_ => { _ = RenewAsync(); }, null, _heartbeatInterval, Timeout.InfiniteTimeSpan);
    }

    private static async Task<(string ownerId, int version, AcquireMode mode, int retryCount, long sleepMs)> AcquireAsync(
        PengdowsCrudJobStorage storage, string resource, TimeSpan timeout)
    {
        var deadline   = DateTime.UtcNow + timeout;
        var ownerId    = Guid.NewGuid().ToString("N");
        var ttl        = storage.Options.DistributedLockTtl;
        var retryDelay = storage.Options.DistributedLockRetryDelay;
        var jitter     = storage.Options.DistributedLockRetryJitter;
        int  retryCount = 0;
        long sleepMs    = 0;

        while (true)
        {
            var now = DateTime.UtcNow;
            var (record, wasSteal) = await storage.Locks.TryAcquireAsync(resource, ownerId, now + ttl, now);
            if (record != null)
            {
                var mode = wasSteal      ? AcquireMode.TtlSteal
                         : retryCount > 0 ? AcquireMode.FollowRelease
                         :                  AcquireMode.InsertWin;
                return (ownerId, record.Version, mode, retryCount, sleepMs);
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new DistributedLockTimeoutException(resource);
            }

            retryCount++;
            var sleep = jitter ? JitteredDelay(retryDelay) : retryDelay;
            sleepMs += (long)sleep.TotalMilliseconds;
            await Task.Delay(sleep);
        }
    }

    /// <summary>
    /// Returns a delay sampled uniformly from [base/2, base*3/2].
    /// Breaks phase alignment: waiters that slept the same interval no longer
    /// wake simultaneously and collide on the same release edge.
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
