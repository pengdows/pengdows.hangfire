using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IJobStateGateway : ITableGateway<State, long>
{
    Task<State?> GetLatestAsync(long jobId, IDatabaseContext? context = null);
    Task<List<State>> GetAllForJobAsync(long jobId);
}
