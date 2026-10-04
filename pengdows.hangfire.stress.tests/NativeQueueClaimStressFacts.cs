using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.hangfire.stress.tests.infrastructure;
using Xunit;

namespace pengdows.hangfire.stress.tests;

[Collection("SqlServerStress")]
public sealed class SqlServerQueueClaimStressFacts
{
    private readonly SqlServerFixture _fixture;

    public SqlServerQueueClaimStressFacts(SqlServerFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 120_000)]
    public async Task ConcurrentClaims_ReturnEachJobAtMostOnce()
    {
        var queue = "native-sqlserver-" + Guid.NewGuid().ToString("N");
        const int jobCount = 100;

        for (var jobId = 1; jobId <= jobCount; jobId++)
        {
            await _fixture.Storage.JobQueues.CreateAsync(new JobQueue { JobID = jobId, Queue = queue });
        }

        var claims = await NativeQueueClaimStressHelpers.FetchClaimsAsync(_fixture.Storage, queue, jobCount);
        var claimedJobIds = claims.Where(claim => claim is not null).Select(claim => claim!.Value.JobId).ToArray();

        Assert.Equal(jobCount, claimedJobIds.Length);
        Assert.Equal(jobCount, claimedJobIds.Distinct().Count());
    }
}

[Collection("PostgresStress")]
public sealed class PostgresQueueClaimStressFacts
{
    private readonly PostgresStressFixture _fixture;

    public PostgresQueueClaimStressFacts(PostgresStressFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 120_000)]
    public async Task ConcurrentClaims_ReturnEachJobAtMostOnce()
    {
        var queue = "native-postgres-" + Guid.NewGuid().ToString("N");
        const int jobCount = 100;

        for (var jobId = 1; jobId <= jobCount; jobId++)
        {
            await _fixture.Storage.JobQueues.CreateAsync(new JobQueue { JobID = jobId, Queue = queue });
        }

        var claims = await NativeQueueClaimStressHelpers.FetchClaimsAsync(_fixture.Storage, queue, jobCount);
        var claimedJobIds = claims.Where(claim => claim is not null).Select(claim => claim!.Value.JobId).ToArray();

        Assert.Equal(jobCount, claimedJobIds.Length);
        Assert.Equal(jobCount, claimedJobIds.Distinct().Count());
    }
}

internal static class NativeQueueClaimStressHelpers
{
    public static Task<(long JobId, string Queue, string FetchToken)?[]> FetchClaimsAsync(
        PengdowsCrudJobStorage storage,
        string queue,
        int workerCount)
    {
        return Task.WhenAll(Enumerable.Range(0, workerCount).Select(async _ =>
        {
            for (var attempt = 0; attempt < 100; attempt++)
            {
                var claim = await storage.JobQueues.FetchNextJobAsync([queue], CancellationToken.None);
                if (claim.HasValue)
                {
                    return claim;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(5));
            }

            return ((long JobId, string Queue, string FetchToken)?)null;
        }));
    }
}
