using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class JobParameterGateway : PrimaryKeyTableGateway<JobParameter>, IJobParameterGateway
{
    public JobParameterGateway(IDatabaseContext context) : base(context) { }

    public Task<Dictionary<string, string>> GetAllForJobAsync(long jobId) => GetAllForJobAsync(jobId, null);

    public async Task<Dictionary<string, string>> GetAllForJobAsync(long jobId, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var sc = BuildBaseRetrieve("p", ctx);
        sc.AppendWhere();
        sc.AppendName("p.JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        var parameters = await LoadListAsync(sc);
        return parameters.ToDictionary(p => p.Name, p => p.Value ?? string.Empty);
    }
}
