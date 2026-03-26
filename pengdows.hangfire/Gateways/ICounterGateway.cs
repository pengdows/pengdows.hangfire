using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface ICounterGateway : ITableGateway<Counter, long>
{
    Task AppendAsync(string key, int delta, DateTime? expireAt = null, IDatabaseContext? context = null);
    Task<int> AggregateAsync(int batchSize);
}
