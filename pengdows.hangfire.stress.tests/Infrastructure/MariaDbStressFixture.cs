using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;
using Testcontainers.MySql;

namespace pengdows.hangfire.stress.tests.infrastructure;

public sealed class MariaDbStressFixture : BaseStressFixture
{
    private MySqlContainer _container = null!;

    protected override string ConnectionString =>
        _container.GetConnectionString() + ";Default Command Timeout=120;Allow User Variables=true";
    protected override DbProviderFactory Factory => MySqlConnectorFactory.Instance;
    protected override int DefaultPoolSize => 4;
    protected override int ConnectionPoolSize => 8;
    protected override System.TimeSpan PoolAcquireTimeout => System.TimeSpan.FromSeconds(120);

    protected override async Task StartContainerAsync()
    {
        _container = new MySqlBuilder("mariadb:11")
            .WithDatabase("HangFire")
            .WithPassword("password")
            .Build();
        await _container.StartAsync();
    }

    protected override async Task StopContainerAsync()
    {
        if (_container != null) await _container.StopAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "MySqlInstall.sql");
}
