using System;
using System.Linq;
using Hangfire.Server;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class CountersAggregatorTests
{
    private static (PengdowsCrudJobStorage Storage, fakeDbFactory Factory) CreateStorage()
    {
        var factory = new fakeDbFactory(SupportedDatabase.PostgreSql);
        var ctx     = new DatabaseContext("Host=fake", factory);
        return (new PengdowsCrudJobStorage(ctx), factory);
    }

    [Fact]
    public void Execute_IssuesAggregateCalls()
    {
        var (storage, factory) = CreateStorage();
        var aggregator = new CountersAggregator(storage, TimeSpan.FromMinutes(1));
        
        // CounterGateway.AggregateAsync expectations:
        // Index 0 (Id): GetInt64
        // Index 1 (Key): GetString
        // Index 2 (Value): GetInt32
        
        var row = new System.Collections.Generic.Dictionary<string, object>();
        row["Id"] = 1L;    
        row["Key"] = "k1"; 
        row["Value"] = 10; 
        
        factory.EnqueueReaderResult(new[] { row });

        using var cts = new System.Threading.CancellationTokenSource();
        var context = new BackgroundProcessContext(
            "serverId",
            storage,
            new System.Collections.Generic.Dictionary<string, object>(),
            Guid.Empty,
            cts.Token,
            System.Threading.CancellationToken.None,
            System.Threading.CancellationToken.None);

        aggregator.Execute(context);

        Assert.True(factory.CreatedConnections.Any());
    }

    [Fact]
    public void Execute_LoopsWhenBatchSizeReached()
    {
        var (storage, factory) = CreateStorage();
        var aggregator = new CountersAggregator(storage, TimeSpan.FromMinutes(1));
        
        // Pass 1: return 1000 rows
        var batch = Enumerable.Range(1, 1000).Select(i => new System.Collections.Generic.Dictionary<string, object> {
            ["Id"] = (long)i,
            ["Key"] = "k",
            ["Value"] = 1
        }).ToList();
        factory.EnqueueReaderResult(batch);
        
        // Pass 2: return 0 rows
        factory.EnqueueReaderResult(Array.Empty<System.Collections.Generic.Dictionary<string, object>>());

        using var cts = new System.Threading.CancellationTokenSource();
        var context = new BackgroundProcessContext(
            "serverId",
            storage,
            new System.Collections.Generic.Dictionary<string, object>(),
            Guid.Empty,
            cts.Token,
            System.Threading.CancellationToken.None,
            System.Threading.CancellationToken.None);

        aggregator.Execute(context);

        Assert.True(factory.CreatedConnections.Count >= 2);
    }

    [Fact]
    public void Constructor_NullStorage_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new CountersAggregator(null!, TimeSpan.FromMinutes(1)));
    }
}
