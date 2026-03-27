using System;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class StorageOptionsTests
{
    [Fact]
    public void DistributedLockTtl_BelowMinimum_Throws()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.DistributedLockTtl = TimeSpan.FromSeconds(4));
    }

    [Fact]
    public void DistributedLockTtl_AtMinimum_DoesNotThrow()
    {
        var opts = new PengdowsCrudStorageOptions();
        opts.DistributedLockTtl = TimeSpan.FromSeconds(5);
        Assert.Equal(TimeSpan.FromSeconds(5), opts.DistributedLockTtl);
    }

    [Fact]
    public void DistributedLockTtl_AboveMinimum_DoesNotThrow()
    {
        var opts = new PengdowsCrudStorageOptions();
        opts.DistributedLockTtl = TimeSpan.FromMinutes(10);
        Assert.Equal(TimeSpan.FromMinutes(10), opts.DistributedLockTtl);
    }

    [Fact]
    public void DistributedLockTtl_Zero_Throws()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.DistributedLockTtl = TimeSpan.Zero);
    }

    [Fact]
    public void DistributedLockRetryDelay_BelowMinimum_Throws()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.DistributedLockRetryDelay = TimeSpan.Zero);
    }

    [Fact]
    public void DistributedLockRetryDelay_AtMinimum_DoesNotThrow()
    {
        var opts = new PengdowsCrudStorageOptions();
        opts.DistributedLockRetryDelay = TimeSpan.FromMilliseconds(1);
        Assert.Equal(TimeSpan.FromMilliseconds(1), opts.DistributedLockRetryDelay);
    }

    [Fact]
    public void DistributedLockRetryDelay_AboveMinimum_DoesNotThrow()
    {
        var opts = new PengdowsCrudStorageOptions();
        opts.DistributedLockRetryDelay = TimeSpan.FromMilliseconds(200);
        Assert.Equal(TimeSpan.FromMilliseconds(200), opts.DistributedLockRetryDelay);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Equal("hangfire", opts.SchemaName);
        Assert.True(opts.AutoPrepareSchema);
        Assert.Equal(TimeSpan.FromSeconds(5), opts.QueuePollInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.InvisibilityTimeout);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.DistributedLockTtl);
        Assert.Equal(TimeSpan.FromMilliseconds(50), opts.DistributedLockRetryDelay);
        Assert.True(opts.DistributedLockRetryJitter);
        Assert.Empty(opts.AdditionalMetricsContexts);
        Assert.Null(opts.TenantRegistry);
    }

    [Fact]
    public void SchemaName_CanBeChanged()
    {
        var opts = new PengdowsCrudStorageOptions { SchemaName = "custom" };
        Assert.Equal("custom", opts.SchemaName);
    }

    [Fact]
    public void UseReadOnlyMonitoring_DefaultTrue()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.True(opts.UseReadOnlyMonitoring);
    }
}
