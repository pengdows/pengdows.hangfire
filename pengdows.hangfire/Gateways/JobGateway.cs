using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class JobGateway : TableGateway<Job, long>, IJobGateway
{
    public JobGateway(IDatabaseContext context) : base(context) { }

    public async Task<int> UpdateExpireAtAsync(long id, DateTime? expireAt, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("ExpireAt").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("expireAt", DbType.DateTime, expireAt as object ?? DBNull.Value));
        sc.AppendWhere();
        sc.AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.Int64, id));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> UpdateStateNameAsync(long id, string stateName, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("StateName").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("stateName", DbType.String, stateName));
        sc.AppendWhere();
        sc.AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.Int64, id));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> UpdateStateAsync(long id, long stateId, string stateName, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("StateId").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("stateId", DbType.Int64, stateId));
        sc.AppendComma();
        sc.AppendName("StateName").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("stateName", DbType.String, stateName));
        sc.AppendWhere();
        sc.AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.Int64, id));
        return await sc.ExecuteNonQueryAsync();
    }

    public Task<List<Job>> GetPagedByStateAsync(string stateName, int from, int count)
        => GetPagedByStateAsync(stateName, from, count, null);

    public async Task<List<Job>> GetPagedByStateAsync(string stateName, int from, int count, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var sc = BuildBaseRetrieve("j", ctx);
        sc.AppendWhere();
        sc.AppendName("j.StateName").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("stateName", DbType.String, stateName));
        sc.AppendQuery(" ORDER BY ").AppendName("j.Id").AppendQuery(" DESC");
        ctx.Dialect.AppendPaging(sc.Query, from, count);
        return await LoadListAsync(sc);
    }

    public Task<int> DeleteExpiredAsync(int batchSize) => DeleteExpiredAsync(batchSize, null);

    public async Task<int> DeleteExpiredAsync(int batchSize, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        var sc = BuildBaseRetrieve("j", ctx);
        sc.AppendWhere();
        sc.AppendName("j.ExpireAt").AppendQuery(" < ").AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendQuery(" ORDER BY ").AppendName("j.Id").AppendQuery(" ASC");
        ctx.Dialect.AppendPaging(sc.Query, 0, batchSize);
        var expired = await LoadListAsync(sc);
        if (expired.Count == 0)
        {
            return 0;
        }

        return await BatchDeleteAsync(expired.Select(j => j.ID), ctx);
    }
}
