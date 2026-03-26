using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface ISetGateway : IPrimaryKeyTableGateway<Set>
{
    Task<HashSet<string>> GetAllItemsAsync(string key);
    Task<string?> GetFirstByLowestScoreAsync(string key, double fromScore, double toScore);
    Task<List<string>> GetFirstByLowestScoreAsync(string key, double fromScore, double toScore, int count);
    Task<long> GetCountAsync(string key);
    Task<bool> ContainsAsync(string key, string value);
    Task<List<string>> GetRangeAsync(string key, int from, int to);
    Task<TimeSpan> GetTtlAsync(string key);
    Task<int> UpdateExpireAtAsync(string key, DateTime? expireAt, IDatabaseContext? context = null);
    Task<int> DeleteByKeyAsync(string key, IDatabaseContext? context = null);
    Task<int> DeleteExpiredAsync(int batchSize);
}
