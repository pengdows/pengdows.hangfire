using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IHashGateway : IPrimaryKeyTableGateway<Hash>
{
    Task<Dictionary<string, string>> GetAllEntriesAsync(string key);
    Task<string?> GetValueAsync(string key, string field);
    Task<long> GetCountAsync(string key);
    Task<TimeSpan> GetTtlAsync(string key);
    Task DeleteAllForKeyAsync(string key, IDatabaseContext? context = null);
    Task<int> UpdateExpireAtAsync(string key, DateTime? expireAt, IDatabaseContext? context = null);
    Task<int> DeleteExpiredAsync(int batchSize);
}
