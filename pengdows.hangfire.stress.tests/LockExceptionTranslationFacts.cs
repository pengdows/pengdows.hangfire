using System;
using System.Data;
using System.Threading.Tasks;
using pengdows.hangfire.stress.tests.infrastructure;
using Hangfire.Storage;
using Xunit;

namespace pengdows.hangfire.stress.tests;

[Collection("SqlServerStress")]
public sealed class LockExceptionTranslationFacts
{
    private readonly SqlServerFixture _f;

    public LockExceptionTranslationFacts(SqlServerFixture fixture)
    {
        _f = fixture;
    }

    [Fact(Timeout = 60_000)]
    public async Task ExpiredLock_CanBeStolen_OnSqlServer_ThroughStorageProvider()
    {
        var resource = "sqlserver-lock-steal-" + Guid.NewGuid().ToString("N");

        await using (var sc = _f.Context.CreateSqlContainer(
                         "INSERT INTO [HangFire].[hf_lock] ([resource], [owner_id], [expires_at], [version]) " +
                         "VALUES (@r, @o, @e, 1)"))
        {
            sc.AddParameterWithValue("r", DbType.String, resource);
            sc.AddParameterWithValue("o", DbType.String, "old-owner");
            sc.AddParameterWithValue("e", DbType.DateTime2, DateTime.UtcNow.AddMinutes(-1));
            await sc.ExecuteNonQueryAsync();
        }

        using var lk = new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.FromSeconds(5));

        Assert.Equal(AcquireMode.TtlSteal, lk.HowAcquired);

        var ownerId = await _f.QueryScalarAsync<string>(
            "SELECT [owner_id] FROM [HangFire].[hf_lock] WHERE [resource] = @r", ("r", resource));
        Assert.False(string.IsNullOrWhiteSpace(ownerId));
        Assert.NotEqual("old-owner", ownerId);
    }

    [Fact(Timeout = 60_000)]
    public async Task HeldLock_TimesOut_OnSqlServer_AndPreservesOriginalOwner()
    {
        var resource = "sqlserver-lock-held-" + Guid.NewGuid().ToString("N");

        await using (var sc = _f.Context.CreateSqlContainer(
                         "INSERT INTO [HangFire].[hf_lock] ([resource], [owner_id], [expires_at], [version]) " +
                         "VALUES (@r, @o, @e, 1)"))
        {
            sc.AddParameterWithValue("r", DbType.String, resource);
            sc.AddParameterWithValue("o", DbType.String, "current-owner");
            sc.AddParameterWithValue("e", DbType.DateTime2, DateTime.UtcNow.AddMinutes(5));
            await sc.ExecuteNonQueryAsync();
        }

        Assert.Throws<DistributedLockTimeoutException>(() =>
            new PengdowsCrudDistributedLock(_f.Storage, resource, TimeSpan.Zero));

        var ownerId = await _f.QueryScalarAsync<string>(
            "SELECT [owner_id] FROM [HangFire].[hf_lock] WHERE [resource] = @r", ("r", resource));
        Assert.Equal("current-owner", ownerId);
    }
}
