using System;
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
}
