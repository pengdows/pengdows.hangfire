using System;
using System.Linq;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class FetchedJobWatchdogTests
{
    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new FetchedJobWatchdog(null!, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void RunOnce_ExecutesSql()
    {
        var (storage, factory) = CreateStorage();
        var watchdog = new FetchedJobWatchdog(storage, TimeSpan.FromMinutes(5));
        watchdog.RunOnce();
        Assert.True(factory.CreatedConnections.Any());
    }

    [Fact]
    public void RunOnce_SqlContainsFetchedAt()
    {
        var (storage, factory) = CreateStorage();
        var watchdog = new FetchedJobWatchdog(storage, TimeSpan.FromMinutes(5));
        watchdog.RunOnce();
        var allSql = factory.CreatedConnections
            .SelectMany(c => c.ExecutedNonQueryTexts)
            .ToList();
        Assert.Contains(allSql, s => s.Contains("FetchedAt", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ToString_ReturnsFriendlyName()
    {
        var (storage, _) = CreateStorage();
        var watchdog = new FetchedJobWatchdog(storage, TimeSpan.FromMinutes(5));
        Assert.Equal(nameof(FetchedJobWatchdog), watchdog.ToString());
    }

    [Fact]
    public void RunOnce_WhenGatewayThrows_DoesNotPropagate()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var storage = new PengdowsCrudJobStorage(ctx);

        // Inject a connection whose ExecuteNonQuery throws; the catch block must swallow it
        var broken = new fakeDbConnection();
        broken.SetNonQueryExecuteException(new InvalidOperationException("DB exploded"));
        factory.Connections.Insert(0, broken);

        var watchdog = new FetchedJobWatchdog(storage, TimeSpan.FromMinutes(5));
        var ex = Record.Exception(() => watchdog.RunOnce());
        Assert.Null(ex);
    }
}
