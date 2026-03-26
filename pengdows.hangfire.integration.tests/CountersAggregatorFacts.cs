using System;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using Xunit;

namespace pengdows.hangfire.integration.tests;

/// <summary>
/// Verifies that CounterGateway.AggregateAsync rolls up Counter rows into AggregatedCounter
/// and removes the source rows.
/// </summary>
public abstract class CountersAggregatorFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _f;

    protected CountersAggregatorFacts(TFixture fixture) => _f = fixture;

    [Fact]
    public async Task Aggregate_SumsCountersIntoAggregatedCounter()
    {
        var key = "agg-sum-" + Guid.NewGuid();
        await _f.InsertCounterAsync(key, 1);
        await _f.InsertCounterAsync(key, 4);
        await _f.InsertCounterAsync(key, 10);

        await _f.Storage.Counters.AggregateAsync(1000);

        var aggregated = await _f.Storage.AggregatedCounters.RetrieveOneAsync(key);
        Assert.NotNull(aggregated);
        Assert.Equal(15, aggregated.Value);
    }

    [Fact]
    public async Task Aggregate_RemovesSourceCounterRows()
    {
        var key = "agg-del-" + Guid.NewGuid();
        await _f.InsertCounterAsync(key, 3);
        await _f.InsertCounterAsync(key, 7);

        await _f.Storage.Counters.AggregateAsync(1000);

        var rows = await _f.Storage.Counters.GetWhereAsync("Key", key);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task Aggregate_HandlesTwoKeysSeparately()
    {
        var key1 = "agg-k1-" + Guid.NewGuid();
        var key2 = "agg-k2-" + Guid.NewGuid();
        await _f.InsertCounterAsync(key1, 5);
        await _f.InsertCounterAsync(key2, 10);
        await _f.InsertCounterAsync(key2, 5);

        await _f.Storage.Counters.AggregateAsync(1000);

        var a1 = await _f.Storage.AggregatedCounters.RetrieveOneAsync(key1);
        var a2 = await _f.Storage.AggregatedCounters.RetrieveOneAsync(key2);
        
        Assert.NotNull(a1);
        Assert.NotNull(a2);
        Assert.Equal(5, a1.Value);
        Assert.Equal(15, a2.Value);
    }

    [Fact]
    public async Task Aggregate_AccumulatesIntoExistingAggregatedCounter()
    {
        var key = "agg-acc-" + Guid.NewGuid();
        // Pre-seed an existing aggregated value
        await _f.Storage.AggregatedCounters.CreateAsync(new AggregatedCounter { Key = key, Value = 100 });
        await _f.InsertCounterAsync(key, 25);

        await _f.Storage.Counters.AggregateAsync(1000);

        var aggregated = await _f.Storage.AggregatedCounters.RetrieveOneAsync(key);
        Assert.NotNull(aggregated);
        Assert.Equal(125, aggregated.Value);
    }
}

[Collection("Sqlite")]
public class SqliteCountersAggregatorFacts : CountersAggregatorFacts<SqliteFixture>
{
    public SqliteCountersAggregatorFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public class PostgresCountersAggregatorFacts : CountersAggregatorFacts<PostgresFixture>
{
    public PostgresCountersAggregatorFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public class SqlServerCountersAggregatorFacts : CountersAggregatorFacts<SqlServerFixture>
{
    public SqlServerCountersAggregatorFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public class MySqlCountersAggregatorFacts : CountersAggregatorFacts<MySqlFixture>
{
    public MySqlCountersAggregatorFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public class OracleCountersAggregatorFacts : CountersAggregatorFacts<OracleFixture>
{
    public OracleCountersAggregatorFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public class FirebirdCountersAggregatorFacts : CountersAggregatorFacts<FirebirdFixture>
{
    public FirebirdCountersAggregatorFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public class CockroachDbCountersAggregatorFacts : CountersAggregatorFacts<CockroachDbFixture>
{
    public CockroachDbCountersAggregatorFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public class MariaDbCountersAggregatorFacts : CountersAggregatorFacts<MariaDbFixture>
{
    public MariaDbCountersAggregatorFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public class DuckDbCountersAggregatorFacts : CountersAggregatorFacts<DuckDbFixture>
{
    public DuckDbCountersAggregatorFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public class YugabyteDbCountersAggregatorFacts : CountersAggregatorFacts<YugabyteDbFixture>
{
    public YugabyteDbCountersAggregatorFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public class TiDbCountersAggregatorFacts : CountersAggregatorFacts<TiDbFixture>
{
    public TiDbCountersAggregatorFacts(TiDbFixture fixture) : base(fixture) { }
}
