using System;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class GeneratedModelTests
{
    [Fact]
    public void AggregatedCounter_ExpireAt_RoundTrips()
    {
        var expireAt = DateTime.UtcNow.AddMinutes(5);
        var model = new AggregatedCounter
        {
            Key = "stats:success",
            Value = 42,
            ExpireAt = expireAt
        };

        Assert.Equal(expireAt, model.ExpireAt);
    }

    [Fact]
    public void Counter_Id_RoundTrips()
    {
        var model = new Counter
        {
            Key = "stats:success",
            Value = 2,
            ExpireAt = DateTime.UtcNow,
            ID = 17
        };

        Assert.Equal(17, model.ID);
    }

    [Fact]
    public void Schema_Version_RoundTrips()
    {
        var model = new Schema { Version = 3 };

        Assert.Equal(3, model.Version);
    }
}
