using System;
using System.Collections.Generic;
using System.Linq;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Tests for PengdowsCrudFetchedJob lifecycle, PengdowsCrudJobStorage feature flags,
/// and PengdowsCrudStorageOptions defaults.
/// </summary>
public sealed class FetchedJobAndStorageTests
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    private static bool NonQueryContains(fakeDbFactory f, string s) =>
        f.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts)
         .Any(t => t.Contains(s, StringComparison.OrdinalIgnoreCase));

    // ── PengdowsCrudFetchedJob ────────────────────────────────────────────────

    [Fact]
    public void FetchedJob_Constructor_NullStorage_Throws()
        => Assert.Throws<ArgumentNullException>(() => new PengdowsCrudFetchedJob(null!, 1L, "q"));

    [Fact]
    public void FetchedJob_JobId_ReturnsStringFormOfLong()
    {
        var (storage, _) = CreateStorage();
        var job = new PengdowsCrudFetchedJob(storage, 42L, "default");
        Assert.Equal("42", job.JobId);
    }

    [Fact]
    public void FetchedJob_Dispose_WhenNotRemoved_IssuesRequeue()
    {
        var (storage, factory) = CreateStorage();
        var job = new PengdowsCrudFetchedJob(storage, 42L, "default");
        // Do NOT call RemoveFromQueue — dispose should requeue (FetchedAt → NULL)
        job.Dispose();
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(NonQueryContains(factory, "NULL"));
    }

    [Fact]
    public void FetchedJob_Dispose_WhenRemoved_IssuesAcknowledge()
    {
        var (storage, factory) = CreateStorage();
        var job = new PengdowsCrudFetchedJob(storage, 42L, "default");
        job.RemoveFromQueue(); // mark for deletion
        job.Dispose();
        Assert.True(NonQueryContains(factory, "DELETE"));
        Assert.True(NonQueryContains(factory, "FetchedAt"));
    }

    [Fact]
    public void FetchedJob_DoubleDispose_IsIdempotent()
    {
        var (storage, factory) = CreateStorage();
        var job = new PengdowsCrudFetchedJob(storage, 1L, "q");
        job.Dispose();
        job.Dispose(); // second dispose must not throw or issue extra SQL

        // Exactly one UPDATE (requeue) should have been issued, not two
        var updateCount = factory.CreatedConnections
            .SelectMany(c => c.ExecutedNonQueryTexts)
            .Count(t => t.Contains("UPDATE", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(1, updateCount);
    }

    [Fact]
    public void FetchedJob_Requeue_DoesNotSetRemoveFlag()
    {
        var (storage, factory) = CreateStorage();
        var job = new PengdowsCrudFetchedJob(storage, 1L, "q");
        job.Requeue(); // Requeue() is currently a no-op implementation
        job.Dispose(); // should fall through to RequeueAsync (UPDATE FetchedAt = NULL)
        Assert.True(NonQueryContains(factory, "UPDATE"));
    }

    // ── PengdowsCrudJobStorage features ──────────────────────────────────────

    [Fact]
    public void Storage_HasFeature_ExtendedApi_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.ExtendedApi));
    }

    [Fact]
    public void Storage_HasFeature_JobQueueProperty_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.JobQueueProperty));
    }

    [Fact]
    public void Storage_HasFeature_ProcessesInsteadOfComponents_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.ProcessesInsteadOfComponents));
    }

    [Fact]
    public void Storage_HasFeature_ConnectionGetUtcDateTime_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.Connection.GetUtcDateTime));
    }

    [Fact]
    public void Storage_HasFeature_ConnectionGetSetContains_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.Connection.GetSetContains));
    }

    [Fact]
    public void Storage_HasFeature_ConnectionLimitedGetSetCount_ReturnsTrue()
    {
        var (storage, _) = CreateStorage();
        Assert.True(storage.HasFeature(Hangfire.Storage.JobStorageFeatures.Connection.LimitedGetSetCount));
    }

    [Fact]
    public void Storage_HasFeature_UnknownFeature_ReturnsFalseOrCallsBase()
    {
        var (storage, _) = CreateStorage();
        // Unknown features should return false (base class default)
        Assert.False(storage.HasFeature("com.example.nonexistent.feature"));
    }

    [Fact]
    public void Storage_GetStorageWideProcesses_ReturnsTwoProcesses()
    {
        var (storage, _) = CreateStorage();
        var processes = storage.GetStorageWideProcesses().ToList();
        Assert.Equal(2, processes.Count);
        Assert.Contains(processes, p => p.ToString() == "ExpirationManager");
        Assert.Contains(processes, p => p.ToString() == "CountersAggregator");
    }

    [Fact]
    public void Storage_GetMonitoringApi_ReturnsNonNull()
    {
        var (storage, _) = CreateStorage();
        var api = storage.GetMonitoringApi();
        Assert.NotNull(api);
    }

    [Fact]
    public void Storage_GetConnection_ReturnsNonNull()
    {
        var (storage, _) = CreateStorage();
        using var conn = storage.GetConnection();
        Assert.NotNull(conn);
    }

    // ── PengdowsCrudStorageOptions defaults ──────────────────────────────────

    [Fact]
    public void StorageOptions_Defaults_AreReasonable()
    {
        var opts = new PengdowsCrudStorageOptions();
        Assert.Equal("hangfire", opts.SchemaName);
        Assert.True(opts.AutoPrepareSchema);
        Assert.Equal(TimeSpan.FromSeconds(5), opts.QueuePollInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.InvisibilityTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), opts.ServerHeartbeatInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), opts.JobExpirationCheckInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.CountersAggregateInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), opts.DistributedLockTtl);
    }

    [Fact]
    public void StorageOptions_DistributedLockTtl_BelowMinimum_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PengdowsCrudStorageOptions { DistributedLockTtl = TimeSpan.FromSeconds(4) });
    }

    [Fact]
    public void StorageOptions_CanBeCustomised()
    {
        var opts = new PengdowsCrudStorageOptions
        {
            SchemaName              = "myapp",
            AutoPrepareSchema       = false,
            QueuePollInterval       = TimeSpan.FromSeconds(15),
            JobExpirationCheckInterval = TimeSpan.FromHours(1)
        };
        Assert.Equal("myapp", opts.SchemaName);
        Assert.False(opts.AutoPrepareSchema);
        Assert.Equal(TimeSpan.FromSeconds(15), opts.QueuePollInterval);
        Assert.Equal(TimeSpan.FromHours(1), opts.JobExpirationCheckInterval);
    }

    // ── PengdowsCrudJobStorage constructor guard ──────────────────────────────

    [Fact]
    public void Storage_NullDb_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new PengdowsCrudJobStorage(null!));
    }

    [Fact]
    public void Storage_WithOptions_DoesNotThrow()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var opts    = new PengdowsCrudStorageOptions { SchemaName = "custom", AutoPrepareSchema = false };
        // Should construct without throwing
        var storage = new PengdowsCrudJobStorage(ctx, opts);
        Assert.NotNull(storage);
    }
}
