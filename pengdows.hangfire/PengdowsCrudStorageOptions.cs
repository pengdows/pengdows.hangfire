using System;
using System.Collections.Generic;
using pengdows.crud;

namespace pengdows.hangfire;

public sealed class PengdowsCrudStorageOptions
{
    [Obsolete("SchemaName is ignored. Custom database schemas are not supported; schema-capable databases always use the built-in HangFire schema.")]
    public string SchemaName { get; set; } = "hangfire";
    public bool AutoPrepareSchema { get; set; } = true;
    public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan DistributedLockRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
    public bool DistributedLockRetryJitter { get; set; } = true;
    public TimeSpan JobExpirationCheckInterval { get; set; } = TimeSpan.FromMinutes(30);
    public TimeSpan CountersAggregateInterval { get; set; } = TimeSpan.FromMinutes(5);

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
    /// When true (default), each empty-queue poll sleep is sampled uniformly
    /// from [interval/2, interval*3/2] to break phase alignment among concurrent
    /// pollers.  Prevents all workers from waking simultaneously and hammering
    /// the same row after a shared idle period.
    ///
    /// Set to false only when deterministic poll timing is required for testing.
    /// </summary>
    public bool QueuePollJitter { get; set; } = true;
}
