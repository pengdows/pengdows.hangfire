namespace pengdows.hangfire;

using System;
using System.Diagnostics;
using System.Threading;
using Hangfire.Logging;

/// <summary>
/// A monotonic UTC clock periodically anchored to the database clock.
/// </summary>
internal sealed class StorageClock
{
    private static readonly ILog Logger = LogProvider.For<StorageClock>();
    private readonly Func<DateTime> _readDbUtc;
    private readonly TimeSpan _refresh;
    private readonly object _gate = new();
    private DateTime _anchorDb;
    private long _anchorTicks;
    private int _refreshing;
    private int _needsInitialRefresh = 1;

    public StorageClock(Func<DateTime> readDbUtc, TimeSpan? refresh = null)
    {
        _readDbUtc = readDbUtc ?? throw new ArgumentNullException(nameof(readDbUtc));
        _refresh = refresh ?? TimeSpan.FromSeconds(60);
        _anchorDb = DateTime.UtcNow;
        _anchorTicks = Stopwatch.GetTimestamp();
    }

    public DateTime UtcNow
    {
        get
        {
            var ticks = Stopwatch.GetTimestamp();
            var elapsed = Stopwatch.GetElapsedTime(_anchorTicks, ticks);
            if (Volatile.Read(ref _needsInitialRefresh) != 0 || elapsed > _refresh)
            {
                TryRefresh();
                ticks = Stopwatch.GetTimestamp();
            }

            lock (_gate)
            {
                return DateTime.SpecifyKind(_anchorDb + Stopwatch.GetElapsedTime(_anchorTicks, ticks), DateTimeKind.Utc);
            }
        }
    }

    private void TryRefresh()
    {
        if (Interlocked.CompareExchange(ref _refreshing, 1, 0) != 0)
            return;

        try
        {
            var start = Stopwatch.GetTimestamp();
            var dbNow = _readDbUtc();
            var end = Stopwatch.GetTimestamp();
            var midpoint = dbNow + Stopwatch.GetElapsedTime(start, end) / 2;
            lock (_gate)
            {
                var oldAtEnd = _anchorDb + Stopwatch.GetElapsedTime(_anchorTicks, end);
                if (midpoint < oldAtEnd)
                    midpoint = oldAtEnd;
                _anchorDb = DateTime.SpecifyKind(midpoint, DateTimeKind.Utc);
                _anchorTicks = end;
            }
            Volatile.Write(ref _needsInitialRefresh, 0);
        }
        catch (Exception ex)
        {
            Logger.WarnException("Unable to refresh the database-anchored storage clock; retaining the last confirmed anchor.", ex);
        }
        finally
        {
            Volatile.Write(ref _needsInitialRefresh, 0);
            Volatile.Write(ref _refreshing, 0);
        }
    }
}
