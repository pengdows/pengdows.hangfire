using System;
using System.Threading.Tasks;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class SchemaInstallerTests
{
    [Fact]
    public async Task InstallAsync_ExecutesSql()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var installer = new PengdowsCrudSchemaInstaller(ctx);

        await installer.InstallAsync();

        var nonQueries = factory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts).ToList();
        Assert.NotEmpty(nonQueries);
        // The SQL script contains CREATE TABLE etc.
        Assert.Contains(nonQueries, t => t.Contains("CREATE TABLE", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Constructor_NullDb_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new PengdowsCrudSchemaInstaller(null!));
    }
}
