using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Storage;
using pengdows.hangfire;
using Xunit;

namespace pengdows.hangfire.integration.tests;

public abstract class ProviderQueueStressFacts<TFixture> where TFixture : StorageFixture
{
    private readonly TFixture _fixture;

    protected ProviderQueueStressFacts(TFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 120_000)]
    [Trait("Category", "ProviderStress")]
    public async Task ConcurrentQueueClaims_ReturnEachJobAtMostOnce()
    {
        var queue = "provider-stress-" + Guid.NewGuid().ToString("N");
        const int jobCount = 32;

        for (var i = 0; i < jobCount; i++)
        {
            var jobId = await _fixture.InsertJobAsync();
            await _fixture.InsertJobQueueAsync(jobId, queue);
        }

        var claims = await Task.WhenAll(Enumerable.Range(0, jobCount).Select(_ => FetchWithRetryAsync(queue)));
        var claimed = claims.Where(claim => claim.HasValue).Select(claim => claim!.Value).ToArray();

        Assert.Equal(jobCount, claimed.Length);
        Assert.Equal(jobCount, claimed.Select(claim => claim.JobId).Distinct().Count());
        Assert.All(claimed, claim => Assert.False(string.IsNullOrWhiteSpace(claim.FetchToken)));
    }

    [Fact(Timeout = 120_000)]
    [Trait("Category", "ProviderStress")]
    public async Task ConcurrentDistributedLocks_NeverOverlapOwnership()
    {
        var resource = "provider-lock-stress-" + Guid.NewGuid().ToString("N");
        const int workerCount = 32;
        var activeOwners = 0;
        var maxOwners = 0;

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
        {
            try
            {
                using var distributedLock = new PengdowsCrudDistributedLock(
                    _fixture.Storage, resource, TimeSpan.FromSeconds(10));

                var owners = Interlocked.Increment(ref activeOwners);
                UpdateMaximum(ref maxOwners, owners);
                await Task.Delay(TimeSpan.FromMilliseconds(10));
                Interlocked.Decrement(ref activeOwners);
            }
            catch (DistributedLockTimeoutException)
            {
            }
        })));

        Assert.Equal(0, activeOwners);
        Assert.True(maxOwners <= 1, $"Concurrent owners reached {maxOwners}.");
    }

    private async Task<(long JobId, string Queue, string FetchToken)?> FetchWithRetryAsync(string queue)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var claim = await _fixture.Storage.JobQueues.FetchNextJobAsync([queue], CancellationToken.None);
            if (claim.HasValue)
            {
                return claim;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        return null;
    }

    private static void UpdateMaximum(ref int target, int value)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (value <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }
}

[Collection("Sqlite")]
public sealed class SqliteProviderQueueStressFacts : ProviderQueueStressFacts<SqliteFixture>
{
    public SqliteProviderQueueStressFacts(SqliteFixture fixture) : base(fixture) { }
}

[Collection("PostgreSql")]
public sealed class PostgresProviderQueueStressFacts : ProviderQueueStressFacts<PostgresFixture>
{
    public PostgresProviderQueueStressFacts(PostgresFixture fixture) : base(fixture) { }
}

[Collection("SqlServer")]
public sealed class SqlServerProviderQueueStressFacts : ProviderQueueStressFacts<SqlServerFixture>
{
    public SqlServerProviderQueueStressFacts(SqlServerFixture fixture) : base(fixture) { }
}

[Collection("MySql")]
public sealed class MySqlProviderQueueStressFacts : ProviderQueueStressFacts<MySqlFixture>
{
    public MySqlProviderQueueStressFacts(MySqlFixture fixture) : base(fixture) { }
}

[Collection("Oracle")]
public sealed class OracleProviderQueueStressFacts : ProviderQueueStressFacts<OracleFixture>
{
    public OracleProviderQueueStressFacts(OracleFixture fixture) : base(fixture) { }
}

[Collection("Firebird")]
public sealed class FirebirdProviderQueueStressFacts : ProviderQueueStressFacts<FirebirdFixture>
{
    public FirebirdProviderQueueStressFacts(FirebirdFixture fixture) : base(fixture) { }
}

[Collection("CockroachDb")]
public sealed class CockroachDbProviderQueueStressFacts : ProviderQueueStressFacts<CockroachDbFixture>
{
    public CockroachDbProviderQueueStressFacts(CockroachDbFixture fixture) : base(fixture) { }
}

[Collection("MariaDb")]
public sealed class MariaDbProviderQueueStressFacts : ProviderQueueStressFacts<MariaDbFixture>
{
    public MariaDbProviderQueueStressFacts(MariaDbFixture fixture) : base(fixture) { }
}

[Collection("DuckDb")]
public sealed class DuckDbProviderQueueStressFacts : ProviderQueueStressFacts<DuckDbFixture>
{
    public DuckDbProviderQueueStressFacts(DuckDbFixture fixture) : base(fixture) { }
}

[Collection("YugabyteDb")]
public sealed class YugabyteDbProviderQueueStressFacts : ProviderQueueStressFacts<YugabyteDbFixture>
{
    public YugabyteDbProviderQueueStressFacts(YugabyteDbFixture fixture) : base(fixture) { }
}

[Collection("TiDb")]
public sealed class TiDbProviderQueueStressFacts : ProviderQueueStressFacts<TiDbFixture>
{
    public TiDbProviderQueueStressFacts(TiDbFixture fixture) : base(fixture) { }
}
