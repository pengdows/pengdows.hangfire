using System;
using System.Linq;
using System.Reflection;
using pengdows.hangfire.gateways;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class GatewayContractTests
{
    [Fact]
    public void GatewayInterfaces_PreserveLegacyMethodSignatures()
    {
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetAllItemsAsync), typeof(string));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetFirstByLowestScoreAsync), typeof(string), typeof(double), typeof(double));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetFirstByLowestScoreAsync), typeof(string), typeof(double), typeof(double), typeof(int));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetCountAsync), typeof(string));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.ContainsAsync), typeof(string), typeof(string));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetRangeAsync), typeof(string), typeof(int), typeof(int));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.GetTtlAsync), typeof(string));
        AssertHasMethod(typeof(ISetGateway), nameof(ISetGateway.DeleteExpiredAsync), typeof(int));

        AssertHasMethod(typeof(IListGateway), nameof(IListGateway.GetCountAsync), typeof(string));
        AssertHasMethod(typeof(IListGateway), nameof(IListGateway.GetTtlAsync), typeof(string));
        AssertHasMethod(typeof(IListGateway), nameof(IListGateway.GetRangeAsync), typeof(string), typeof(int), typeof(int));
        AssertHasMethod(typeof(IListGateway), nameof(IListGateway.GetAllAsync), typeof(string));
        AssertHasMethod(typeof(IListGateway), nameof(IListGateway.DeleteExpiredAsync), typeof(int));

        AssertHasMethod(typeof(IHashGateway), nameof(IHashGateway.GetAllEntriesAsync), typeof(string));
        AssertHasMethod(typeof(IHashGateway), nameof(IHashGateway.GetValueAsync), typeof(string), typeof(string));
        AssertHasMethod(typeof(IHashGateway), nameof(IHashGateway.GetCountAsync), typeof(string));
        AssertHasMethod(typeof(IHashGateway), nameof(IHashGateway.GetTtlAsync), typeof(string));
        AssertHasMethod(typeof(IHashGateway), nameof(IHashGateway.DeleteExpiredAsync), typeof(int));

        AssertHasMethod(typeof(IAggregatedCounterGateway), nameof(IAggregatedCounterGateway.GetTimelineAsync), typeof(string[]));
        AssertHasMethod(typeof(IAggregatedCounterGateway), nameof(IAggregatedCounterGateway.GetValueAsync), typeof(string));
        AssertHasMethod(typeof(IAggregatedCounterGateway), nameof(IAggregatedCounterGateway.DeleteExpiredAsync), typeof(int));

        AssertHasMethod(typeof(IJobGateway), nameof(IJobGateway.GetPagedByStateAsync), typeof(string), typeof(int), typeof(int));
        AssertHasMethod(typeof(IJobGateway), nameof(IJobGateway.DeleteExpiredAsync), typeof(int));

        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.FetchNextJobAsync), typeof(string[]), typeof(System.Threading.CancellationToken));
        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.GetDistinctQueuesAsync));
        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.AcknowledgeAsync), typeof(long), typeof(string));
        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.RequeueAsync), typeof(long), typeof(string));
        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.RequeueStaleAsync), typeof(DateTime));
        AssertHasMethod(typeof(IJobQueueGateway), nameof(IJobQueueGateway.GetPagedByQueueAsync), typeof(string), typeof(int), typeof(int), typeof(bool));

        AssertHasMethod(typeof(IServerGateway), nameof(IServerGateway.RemoveTimedOutAsync), typeof(DateTime));
        AssertHasMethod(typeof(IServerGateway), nameof(IServerGateway.UpdateHeartbeatAsync), typeof(string));

        AssertHasMethod(typeof(IJobStateGateway), nameof(IJobStateGateway.GetAllForJobAsync), typeof(long));
        AssertHasMethod(typeof(IJobParameterGateway), nameof(IJobParameterGateway.GetAllForJobAsync), typeof(long));
        AssertHasMethod(typeof(ICounterGateway), nameof(ICounterGateway.AggregateAsync), typeof(int));

        AssertHasMethod(typeof(IDistributedLockGateway), nameof(IDistributedLockGateway.TryAcquireAsync), typeof(string), typeof(string), typeof(DateTime), typeof(DateTime));
        AssertHasMethod(typeof(IDistributedLockGateway), nameof(IDistributedLockGateway.TryRenewAsync), typeof(string), typeof(string), typeof(int), typeof(DateTime));
        AssertHasMethod(typeof(IDistributedLockGateway), nameof(IDistributedLockGateway.ReleaseAsync), typeof(string), typeof(string));
    }

    private static void AssertHasMethod(Type type, string methodName, params Type[] parameterTypes)
    {
        var method = type.GetMethod(methodName, BindingFlags.Public | BindingFlags.Instance, parameterTypes);
        Assert.True(method != null, $"{type.Name}.{methodName}({string.Join(", ", parameterTypes.Select(t => t.Name))}) was not found.");
    }
}
