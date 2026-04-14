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
    public void Defaults_HaveExpectedValues()
    {
        var opts = new PengdowsCrudStorageOptions();
#pragma warning disable CS0618
        Assert.Equal("hangfire", opts.SchemaName);
#pragma warning restore CS0618
        Assert.True(opts.AutoPrepareSchema);
        Assert.Equal(TimeSpan.FromSeconds(5), opts.QueuePollInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.InvisibilityTimeout);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.DistributedLockTtl);
        Assert.Equal(TimeSpan.FromMilliseconds(100), opts.DistributedLockRetryDelay);
        Assert.True(opts.DistributedLockRetryJitter);
        Assert.True(opts.QueuePollJitter);
        Assert.Empty(opts.AdditionalMetricsContexts);
    }

    [Fact]
    public void InvisibilityTimeout_DefaultIs5Minutes()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Equal(TimeSpan.FromMinutes(5), opts.InvisibilityTimeout);
    }

    [Fact]
    public void InvisibilityTimeout_CanBeSet()
    {
        var opts = new PengdowsCrudStorageOptions { InvisibilityTimeout = TimeSpan.FromMinutes(10) };
        Assert.Equal(TimeSpan.FromMinutes(10), opts.InvisibilityTimeout);
    }

    [Fact]
    public void DistributedLockRetryDelay_DefaultIs100ms()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Equal(TimeSpan.FromMilliseconds(100), opts.DistributedLockRetryDelay);
    }

    [Fact]
    public void DistributedLockRetryDelay_CanBeSet()
    {
        var opts = new PengdowsCrudStorageOptions { DistributedLockRetryDelay = TimeSpan.FromMilliseconds(200) };
        Assert.Equal(TimeSpan.FromMilliseconds(200), opts.DistributedLockRetryDelay);
    }

    [Fact]
    public void DistributedLockRetryJitter_DefaultIsTrue()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.True(opts.DistributedLockRetryJitter);
    }

    [Fact]
    public void DistributedLockRetryJitter_CanBeDisabled()
    {
        var opts = new PengdowsCrudStorageOptions { DistributedLockRetryJitter = false };
        Assert.False(opts.DistributedLockRetryJitter);
    }

    [Fact]
    public void QueuePollJitter_CanBeDisabled()
    {
        var opts = new PengdowsCrudStorageOptions { QueuePollJitter = false };
        Assert.False(opts.QueuePollJitter);
    }

    [Fact]
    public void SchemaName_IsObsoleteAndIgnored()
    {
#pragma warning disable CS0618
        var opts = new PengdowsCrudStorageOptions { SchemaName = "custom" };
        Assert.Equal("custom", opts.SchemaName);
#pragma warning restore CS0618
    }

}
