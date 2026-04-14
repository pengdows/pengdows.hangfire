using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;
using Testcontainers.PostgreSql;

namespace pengdows.hangfire.stress.tests.infrastructure;

public sealed class PostgresStressFixture : BaseStressFixture
{
    private PostgreSqlContainer _container = null!;

    protected override string ConnectionString => _container.GetConnectionString();
    protected override DbProviderFactory Factory => NpgsqlFactory.Instance;
    protected override string PoolSizeKeyword => "Maximum Pool Size";

    protected override async Task StartContainerAsync()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithCommand("-c", "max_connections=300")
            .Build();
        await _container.StartAsync();
    }

    protected override async Task StopContainerAsync()
    {
        if (_container != null) await _container.StopAsync();
    }

    protected override Task InstallSchemaAsync() =>
        SchemaHelper.InstallFromResourceAsync(Context, "PostgresInstall.sql");
}
