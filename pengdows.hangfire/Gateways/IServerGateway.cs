using HfServer = pengdows.hangfire.models.Server;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public interface IServerGateway : ITableGateway<HfServer, string>
{
    Task<int> RemoveTimedOutAsync(DateTime cutoff);
    Task<int> UpdateHeartbeatAsync(string serverId);
}
