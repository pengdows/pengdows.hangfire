using System.Linq;
using System.Threading;
using pengdows.hangfire.models;
using pengdows.hangfire.stress.tests.infrastructure;
using Xunit;

namespace pengdows.hangfire.stress.tests;

public abstract class MySqlFamilyQueueClaimStressFacts<TFixture> where TFixture : BaseStressFixture
{
    private readonly TFixture _fixture;

    protected MySqlFamilyQueueClaimStressFacts(TFixture fixture) => _fixture = fixture;

    [Fact(Timeout = 120_000)]
    public async Task ConcurrentClaims_ReturnEachJobAtMostOnce()
    {
        var queue = $"stress-{Guid.NewGuid():N}";
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

[Collection("MySqlStress")]
public sealed class MySqlQueueClaimStressFacts : MySqlFamilyQueueClaimStressFacts<MySqlStressFixture>
{
    public MySqlQueueClaimStressFacts(MySqlStressFixture fixture) : base(fixture) { }
}

[Collection("MariaDbStress")]
public sealed class MariaDbQueueClaimStressFacts : MySqlFamilyQueueClaimStressFacts<MariaDbStressFixture>
{
    public MariaDbQueueClaimStressFacts(MariaDbStressFixture fixture) : base(fixture) { }
}
