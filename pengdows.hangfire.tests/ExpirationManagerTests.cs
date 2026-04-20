using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Server;
using Hangfire.Storage;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.exceptions;
using pengdows.crud.fakeDb;
using pengdows.hangfire.gateways;
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

    private static BackgroundProcessContext CreateProcessContext(PengdowsCrudJobStorage storage, CancellationToken stoppingToken = default)
        => new("server-1", storage, new Dictionary<string, object>(), Guid.NewGuid(), stoppingToken, CancellationToken.None, CancellationToken.None);

    [Fact]
    public void RunOnce_IssuesDeleteCalls()
    {
        var (storage, factory) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.FromMinutes(1));
        
        factory.EnqueueReaderResult(Array.Empty<System.Collections.Generic.Dictionary<string, object>>());
        for (int i = 0; i < 5; i++)
        {
            factory.EnqueueReaderResult(new[] { new System.Collections.Generic.Dictionary<string, object> { ["Value"] = 0L } });
        }

        manager.RunOnce();
        Assert.True(factory.CreatedConnections.Any());
    }

    [Fact]
    public void RunOnce_HandlesLockTimeoutGracefully()
    {
        var (storage, _) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.FromMinutes(1));
        ReplaceLockGateway(storage, ThrowingLockGatewayProxy.Create("locks:expirationmanager"));

        manager.RunOnce();
    }

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ExpirationManager(null!, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void Execute_RunsCleanup_ThenWaitsForInterval()
    {
        var (storage, factory) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.Zero);

        factory.EnqueueReaderResult(Array.Empty<Dictionary<string, object>>());
        for (int i = 0; i < 5; i++)
        {
            factory.EnqueueReaderResult(new[] { new Dictionary<string, object> { ["Value"] = 0L } });
        }

        var context = CreateProcessContext(storage);
        manager.Execute(context);

        Assert.True(factory.CreatedConnections.Any());
    }

    [Fact]
    public void DeleteExpiredRows_WhenDeleteThrows_IsSwallowed()
    {
        var (storage, _) = CreateStorage();
        var manager = new ExpirationManager(storage, TimeSpan.FromMinutes(1));
        var method = typeof(ExpirationManager).GetMethod("DeleteExpiredRows", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var ex = Record.Exception(() => method.Invoke(manager, ["BrokenTable", new Func<int>(() => throw new InvalidOperationException("boom"))]));

        Assert.Null(ex);
    }

    private static void ReplaceLockGateway(PengdowsCrudJobStorage storage, IDistributedLockGateway gateway)
    {
        var field = typeof(PengdowsCrudJobStorage).GetField("<Locks>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)!;
        field.SetValue(storage, gateway);
    }

    private class ThrowingLockGatewayProxy : DispatchProxy
    {
        public static string Resource { get; set; } = string.Empty;

        public static IDistributedLockGateway Create(string resource)
        {
            Resource = resource;
            return Create<IDistributedLockGateway, ThrowingLockGatewayProxy>();
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name == nameof(IDistributedLockGateway.TryAcquireAsync))
            {
                throw new DistributedLockTimeoutException(Resource);
            }

            if (targetMethod?.ReturnType == typeof(Task<bool>))
            {
                return Task.FromResult(false);
            }

            if (targetMethod?.ReturnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            return null;
        }
    }
}
