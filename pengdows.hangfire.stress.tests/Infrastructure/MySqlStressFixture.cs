using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;
using Testcontainers.MySql;

namespace pengdows.hangfire.stress.tests.infrastructure;

public sealed class MySqlStressFixture : BaseStressFixture
{
    private MySqlContainer _container = null!;

    protected override string ConnectionString => _container.GetConnectionString();
    protected override DbProviderFactory Factory => MySqlConnectorFactory.Instance;

    protected override async Task StartContainerAsync()
    {
        _container = new MySqlBuilder()
            .WithImage("mysql:8.0")
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
