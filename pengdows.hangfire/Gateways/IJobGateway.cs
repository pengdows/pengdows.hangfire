using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IJobGateway : ITableGateway<Job, long>
{
    Task<int> UpdateExpireAtAsync(long id, DateTime? expireAt, IDatabaseContext? context = null);
    Task<int> UpdateStateNameAsync(long id, string stateName, IDatabaseContext? context = null);
    Task<int> UpdateStateAsync(long id, long stateId, string stateName, IDatabaseContext? context = null);
    Task<List<Job>> GetPagedByStateAsync(string stateName, int from, int count);
    Task<int> DeleteExpiredAsync(int batchSize);
}
