using System;
using System.Linq;
using Hangfire.Server;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class ExpirationManagerTests
{
    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    [Fact]
    public void Execute_IssuesDeleteCalls()
    {
        var (storage, factory) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.FromMinutes(1));
        
        factory.EnqueueReaderResult(Array.Empty<System.Collections.Generic.Dictionary<string, object>>());
        for (int i = 0; i < 5; i++)
        {
            factory.EnqueueReaderResult(new[] { new System.Collections.Generic.Dictionary<string, object> { ["Value"] = 0L } });
        }

        using var cts = new System.Threading.CancellationTokenSource();
        var context = new BackgroundProcessContext(
            "serverId",
            storage,
            new System.Collections.Generic.Dictionary<string, object>(),
            Guid.Empty,
            cts.Token,
            System.Threading.CancellationToken.None,
            System.Threading.CancellationToken.None);

        manager.Execute(context);
        Assert.True(factory.CreatedConnections.Any());
    }

    [Fact]
    public void Execute_HandlesLockTimeoutGracefully()
    {
        var (storage, factory) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.FromMinutes(1));
        
        factory.EnqueueReaderResult(new[] { 
            new System.Collections.Generic.Dictionary<string, object> { 
                ["Resource"] = "locks:expirationmanager", 
                ["Id"] = "other-guy",
                ["CreatedAt"] = DateTime.UtcNow
            } 
        });
        
        using var cts = new System.Threading.CancellationTokenSource();
        var context = new BackgroundProcessContext(
            "serverId",
            storage,
            new System.Collections.Generic.Dictionary<string, object>(),
            Guid.Empty,
            cts.Token,
            System.Threading.CancellationToken.None,
            System.Threading.CancellationToken.None);

        manager.Execute(context);
    }

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ExpirationManager(null!, TimeSpan.FromMinutes(1)));
    }
}
