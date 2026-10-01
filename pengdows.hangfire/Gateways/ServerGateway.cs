using System.Data;
using HfServer = pengdows.hangfire.models.Server;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class ServerGateway : TableGateway<HfServer, string>, IServerGateway
{
    private readonly Func<DateTime> _utcNow;
    public ServerGateway(IDatabaseContext context, Func<DateTime>? utcNow = null) : base(context)
        => _utcNow = utcNow ?? (() => DateTime.UtcNow);

    public Task<int> RemoveTimedOutAsync(DateTime cutoff) => RemoveTimedOutAsync(cutoff, null);

    public async Task<int> RemoveTimedOutAsync(DateTime cutoff, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("LastHeartbeat").AppendQuery(" < ")
          .AppendParam(sc.AddParameterWithValue("cutoff", DbType.DateTime, cutoff));
        return await sc.ExecuteNonQueryAsync();
    }

    public Task<int> UpdateHeartbeatAsync(string serverId) => UpdateHeartbeatAsync(serverId, null);

    public async Task<int> UpdateHeartbeatAsync(string serverId, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("LastHeartbeat").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, _utcNow()));
        sc.AppendWhere();
        sc.AppendName("Id").AppendEquals().AppendParam(sc.AddParameterWithValue("id", DbType.String, serverId));
        return await sc.ExecuteNonQueryAsync();
    }
}
