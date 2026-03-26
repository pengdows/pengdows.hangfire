using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IListGateway : ITableGateway<List, long>
{
    Task AppendAsync(string key, string value, IDatabaseContext? context = null);
    Task DeleteByKeyValueAsync(string key, string value, IDatabaseContext? context = null);
    Task TrimAsync(string key, int keepStartingFrom, int keepEndingAt, IDatabaseContext? context = null);
    Task<long> GetCountAsync(string key);
    Task<TimeSpan> GetTtlAsync(string key);
    Task<List<string>> GetRangeAsync(string key, int from, int to);
    Task<List<string>> GetAllAsync(string key);
    Task<int> UpdateExpireAtAsync(string key, DateTime? expireAt, IDatabaseContext? context = null);
    Task<int> DeleteExpiredAsync(int batchSize);
}
