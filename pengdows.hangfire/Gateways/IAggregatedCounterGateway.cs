using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IAggregatedCounterGateway : ITableGateway<AggregatedCounter, string>
{
    Task<Dictionary<string, long>> GetTimelineAsync(string[] keys);
    Task<long> GetValueAsync(string key);
    Task<int> DeleteExpiredAsync(int batchSize);
}
