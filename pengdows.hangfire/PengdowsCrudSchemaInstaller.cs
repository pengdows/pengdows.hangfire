namespace pengdows.hangfire;

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using pengdows.crud;

public sealed class PengdowsCrudSchemaInstaller
{
    private readonly IDatabaseContext _db;

    public PengdowsCrudSchemaInstaller(IDatabaseContext db)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
    }

    public async Task InstallAsync()
    {
        var assembly = Assembly.GetExecutingAssembly();
        await using var stream = assembly.GetManifestResourceStream(
            "pengdows.hangfire.DefaultInstall.sql")
            ?? throw new InvalidOperationException("Embedded resource DefaultInstall.sql not found.");

        using var reader = new StreamReader(stream);
        var sql = await reader.ReadToEndAsync();

        await using var sc = _db.CreateSqlContainer(sql);
        await sc.ExecuteNonQueryAsync();
    }
}
