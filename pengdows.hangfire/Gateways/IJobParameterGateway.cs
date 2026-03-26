using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IJobParameterGateway : IPrimaryKeyTableGateway<JobParameter>
{
    Task<Dictionary<string, string>> GetAllForJobAsync(long jobId);
}
