using System;
using System.Collections.Generic;
using pengdows.crud;
using pengdows.crud.tenant;

namespace pengdows.hangfire;

public sealed class PengdowsCrudStorageOptions
{
    public string SchemaName { get; set; } = "hangfire";
    public bool AutoPrepareSchema { get; set; } = true;
    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan ServerHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    public bool UseReadOnlyMonitoring { get; set; } = true;
    public TimeSpan JobExpirationCheckInterval { get; set; } = TimeSpan.FromMinutes(30);
    public TimeSpan CountersAggregateInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Optional registry of tenant contexts. If provided, metrics for all active
    /// tenants will be included in the monitoring API and dashboard.
    /// </summary>
    public ITenantContextRegistry? TenantRegistry { get; set; }

    /// <summary>
    /// Optional list of additional database contexts to include in metrics reporting.
    /// Use this to include your application's business databases in the Hangfire dashboard.
    /// </summary>
    public IList<IDatabaseContext> AdditionalMetricsContexts { get; } = new List<IDatabaseContext>();

    private TimeSpan _distributedLockTtl = TimeSpan.FromMinutes(5);
    public TimeSpan DistributedLockTtl
    {
        get => _distributedLockTtl;
        set
        {
            if (value < TimeSpan.FromSeconds(5))
            {
                throw new ArgumentOutOfRangeException(nameof(value), "DistributedLockTtl must be at least 5 seconds.");
            }
            _distributedLockTtl = value;
        }
    }

    /// <summary>
    /// How long the acquire loop sleeps between retries when the lock row is
    /// held by another owner and has not yet expired.
    ///
    /// Shorter values wake waiters sooner after a release but increase DB read
    /// traffic under contention.  Longer values reduce traffic but quantize
    /// acquisition latency in multiples of this interval.
    ///
    /// Default: 100 ms.  Minimum: 1 ms.
    /// </summary>
    private TimeSpan _distributedLockRetryDelay = TimeSpan.FromMilliseconds(50);
    public TimeSpan DistributedLockRetryDelay
    {
        get => _distributedLockRetryDelay;
        set
        {
            if (value < TimeSpan.FromMilliseconds(1))
            {
                throw new ArgumentOutOfRangeException(nameof(value), "DistributedLockRetryDelay must be at least 1 ms.");
            }
            _distributedLockRetryDelay = value;
        }
    }

    /// <summary>
    /// When true (default), each retry sleep is sampled uniformly from
    /// [delay/2, delay*3/2] to break phase alignment among concurrent waiters.
    /// This reduces herd collisions after a lock release at the cost of a
    /// slightly wider per-retry latency window.
    ///
    /// Set to false only when deterministic retry timing is required for testing.
    /// </summary>
    public bool DistributedLockRetryJitter { get; set; } = true;
}
