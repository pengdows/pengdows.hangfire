using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class JobParameterGateway : PrimaryKeyTableGateway<JobParameter>, IJobParameterGateway
{
    public JobParameterGateway(IDatabaseContext context) : base(context) { }

    public async Task<Dictionary<string, string>> GetAllForJobAsync(long jobId)
    {
        var sc = BuildBaseRetrieve("p");
        sc.AppendWhere();
        sc.AppendName("p.JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        var parameters = await LoadListAsync(sc);
        return parameters.ToDictionary(p => p.Name, p => p.Value ?? string.Empty);
    }
}
