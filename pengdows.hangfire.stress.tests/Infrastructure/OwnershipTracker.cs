using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace pengdows.hangfire.stress.tests.infrastructure;

/// <summary>
/// In-process invariant checker for distributed lock correctness.
///
/// Three independent detection mechanisms run in parallel:
///
///   1. Live CAS check — <see cref="Enter"/> uses TryAdd on a
///      ConcurrentDictionary; a second caller arriving while the first is
///      still inside is a violation (increments <see cref="Violations"/>).
///
///   2. Live concurrent-count tracking — a per-resource active counter is
///      incremented on Enter and decremented on Exit.  The running maximum
///      is recorded; <see cref="GlobalMaxConcurrentOwners"/> exposes it.
///      If it ever exceeds 1, mutual exclusion was broken.
///
///   3. Post-run interval overlap analysis — every hold interval is recorded;
///      after the run, <see cref="CountIntervalOverlaps"/> counts overlapping
///      intervals on the same resource.  This catches cases where (1) or (2)
///      somehow race themselves (belt + suspenders + strap).
///
/// Correct lock behaviour means:
///   Violations == 0
///   CountIntervalOverlaps() == 0
///   GlobalMaxConcurrentOwners() &lt;= 1
/// </summary>
public sealed class OwnershipTracker
{
    // Mechanism 1: live CAS per resource
    private readonly ConcurrentDictionary<string, string> _current = new();

    // Mechanism 2: per-resource active count + running maximum
    private readonly ConcurrentDictionary<string, int> _activeCount = new();
    private readonly ConcurrentDictionary<string, int> _maxPerResource = new();

    // Mechanism 3: post-run interval analysis
    private readonly ConcurrentBag<IntervalRecord> _intervals = new();

    private long _violations;

    /// <summary>
    /// Must be called immediately after the distributed-lock constructor returns
    /// (i.e. inside the critical section, before doing work).
    /// Returns <c>false</c> and increments <see cref="Violations"/> if another
    /// trackingId is already registered for this resource.
    /// </summary>
    public bool Enter(string resource, string trackingId)
    {
        // Mechanism 2: bump active count and record max
        var active = _activeCount.AddOrUpdate(resource, 1, (_, old) => old + 1);
        _maxPerResource.AddOrUpdate(resource, active, (_, m) => active > m ? active : m);

        // Mechanism 1: CAS — only one trackingId may hold per resource
        if (_current.TryAdd(resource, trackingId))
        {
            return true;
        }

        Interlocked.Increment(ref _violations);
        return false;
    }

    /// <summary>
    /// Must be called immediately before the lock is released (inside the
    /// <c>using</c> block, before the closing brace).
    /// </summary>
    public void Exit(string resource, string trackingId, DateTime enteredAt, DateTime exitedAt)
    {
        _current.TryRemove(new KeyValuePair<string, string>(resource, trackingId));
        _activeCount.AddOrUpdate(resource, 0, (_, old) => old > 0 ? old - 1 : 0);
        _intervals.Add(new IntervalRecord(resource, trackingId, enteredAt, exitedAt));
    }

    /// <summary>Number of live CAS violations detected during the run.</summary>
    public long Violations => Interlocked.Read(ref _violations);

    /// <summary>
    /// Highest number of concurrent owners seen across all resources.
    /// A value &gt; 1 is direct evidence of mutual exclusion failure.
    /// </summary>
    public int GlobalMaxConcurrentOwners() =>
        _maxPerResource.IsEmpty ? 0 : _maxPerResource.Values.Max();

    /// <summary>
    /// Highest number of concurrent owners seen for a specific resource.
    /// </summary>
    public int MaxConcurrentOwners(string resource) =>
        _maxPerResource.TryGetValue(resource, out var m) ? m : 0;

    /// <summary>
    /// Post-run interval overlap count across all resources.
    /// For each resource, checks whether any two recorded hold intervals overlap.
    /// Overlapping intervals mean two holders were live at the same time.
    /// </summary>
    public int CountIntervalOverlaps()
    {
        int total = 0;
        foreach (var group in _intervals.GroupBy(r => r.Resource))
        {
            var sorted = group.OrderBy(r => r.EnteredAt).ToList();
            for (int i = 0; i < sorted.Count - 1; i++)
            {
                if (sorted[i].ExitedAt > sorted[i + 1].EnteredAt)
                {
                    total++;
                }
            }
        }
        return total;
    }

    public sealed record IntervalRecord(
        string   Resource,
        string   TrackingId,
        DateTime EnteredAt,
        DateTime ExitedAt);
}
