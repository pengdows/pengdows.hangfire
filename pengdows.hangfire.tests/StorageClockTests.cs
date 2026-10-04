using System;
using System.Diagnostics;
using System.Threading;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class StorageClockTests
{
    [Fact]
    public void UtcNow_AnchorsToDatabaseAndRemainsMonotonic()
    {
        var databaseNow = DateTime.UtcNow.AddMinutes(3);
        var clock = new StorageClock(() => databaseNow, TimeSpan.Zero);

        var first = clock.UtcNow;
        var second = clock.UtcNow;

        Assert.Equal(DateTimeKind.Utc, first.Kind);
        Assert.InRange(first, databaseNow.AddSeconds(-1), databaseNow.AddSeconds(1));
        Assert.True(second >= first);
    }

    [Fact]
    public void UtcNow_UsesDatabaseTimeWhenNodeClockIsAhead()
    {
        var databaseNow = DateTime.UtcNow.AddMinutes(-3);
        var clock = new StorageClock(() => databaseNow, TimeSpan.Zero);

        var actual = clock.UtcNow;

        Assert.InRange(actual, databaseNow.AddSeconds(-1), databaseNow.AddSeconds(1));
    }

    [Fact]
    public void UtcNow_KeepsLastAnchorWhenDatabaseRefreshFails()
    {
        var databaseNow = DateTime.UtcNow.AddMinutes(3);
        var calls = 0;
        var clock = new StorageClock(() =>
        {
            if (++calls > 1) throw new InvalidOperationException("database unavailable");
            return databaseNow;
        }, TimeSpan.Zero);

        var first = clock.UtcNow;
        var second = clock.UtcNow;

        Assert.InRange(first, databaseNow.AddSeconds(-1), databaseNow.AddSeconds(1));
        Assert.True(second >= first);
        Assert.True(second < databaseNow.AddSeconds(2));
    }

    [Fact]
    public void UtcNow_DoesNotWaitForPeriodicDatabaseRefresh()
    {
        var calls = 0;
        using var refreshStarted = new ManualResetEventSlim();
        using var releaseRefresh = new ManualResetEventSlim();
        var clock = new StorageClock(() =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                return DateTime.UtcNow;
            }

            refreshStarted.Set();
            releaseRefresh.Wait(TimeSpan.FromSeconds(5));
            return DateTime.UtcNow;
        }, TimeSpan.Zero);

        _ = clock.UtcNow;
        _ = clock.UtcNow;
        Assert.True(refreshStarted.Wait(TimeSpan.FromSeconds(1)));

        var stopwatch = Stopwatch.StartNew();
        _ = clock.UtcNow;
        stopwatch.Stop();

        releaseRefresh.Set();
        Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(250),
            $"UtcNow waited {stopwatch.Elapsed.TotalMilliseconds:F0} ms for refresh.");
    }
}
