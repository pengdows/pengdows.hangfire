using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class JobStateGateway : TableGateway<State, long>, IJobStateGateway
{
    public JobStateGateway(IDatabaseContext context) : base(context) { }

    public async Task<State?> GetLatestAsync(long jobId, IDatabaseContext? context = null)
    {
        var sc = BuildBaseRetrieve("s", context);
        sc.AppendWhere();
        sc.AppendName("s.JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendQuery(" ORDER BY ").AppendName("s.Id").AppendQuery(" DESC");
        return await LoadSingleAsync(sc);
    }

    public async Task<List<State>> GetAllForJobAsync(long jobId)
    {
        var sc = BuildBaseRetrieve("s");
        sc.AppendWhere();
        sc.AppendName("s.JobId").AppendEquals().AppendParam(sc.AddParameterWithValue("jobId", DbType.Int64, jobId));
        sc.AppendQuery(" ORDER BY ").AppendName("s.Id").AppendQuery(" DESC");
        return await LoadListAsync(sc);
    }
}
