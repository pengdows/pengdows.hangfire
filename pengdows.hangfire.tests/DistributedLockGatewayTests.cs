using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.exceptions;
using pengdows.crud.fakeDb;
using pengdows.hangfire.gateways;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class DistributedLockGatewayTests
{
    private static (DatabaseContext Context, fakeDbFactory Factory) MakeContext(SupportedDatabase db)
    {
        var factory = new fakeDbFactory(db);
        var connStr = db == SupportedDatabase.PostgreSql ? "Host=fake" : "Data Source=fake";
        return (new DatabaseContext(connStr, factory), factory);
    }

    private static bool NonQueryContains(fakeDbFactory factory, string text) =>
        factory.CreatedConnections
            .SelectMany(c => c.ExecutedNonQueryTexts)
            .Any(sql => sql.Contains(text, StringComparison.OrdinalIgnoreCase));

    [Fact]
    public async Task TryAcquireAsync_PostgreSql_UsesOnConflictWhere()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.PostgreSql);
        await using (ctx)
        {
            var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
                "res",
                "owner",
                DateTime.UtcNow.AddMinutes(1),
                DateTime.UtcNow);

            Assert.True(acquired);
            Assert.True(NonQueryContains(factory, "ON CONFLICT"));
            Assert.True(NonQueryContains(factory, "DO UPDATE"));
            Assert.True(NonQueryContains(factory, "WHERE"));
            Assert.False(NonQueryContains(factory, "MERGE"));
        }
    }

    [Fact]
    public async Task TryAcquireAsync_Sqlite_WhenStealSucceeds_DoesNotAttemptInsert()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.Sqlite);
        await using (ctx)
        {
            var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
                "res",
                "owner",
                DateTime.UtcNow.AddMinutes(1),
                DateTime.UtcNow);

            Assert.True(acquired);
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.False(NonQueryContains(factory, "DO NOTHING"));
        }
    }

    [Fact]
    public async Task TryAcquireAsync_Sqlite_WhenStealMisses_FallsBackToInsertDoNothing()
    {
        var factory = new fakeDbFactory(SupportedDatabase.Sqlite);
        await using var ctx = new DatabaseContext("Data Source=fake", factory);

        var missConn = new fakeDbConnection();
        missConn.NonQueryResults.Enqueue(0);
        factory.Connections.Insert(0, missConn);
        var insertConn = new fakeDbConnection();
        insertConn.NonQueryResults.Enqueue(1);
        factory.Connections.Add(insertConn);

        var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
            "res",
            "owner",
            DateTime.UtcNow.AddMinutes(1),
            DateTime.UtcNow);

        Assert.True(acquired);
        Assert.True(NonQueryContains(factory, "UPDATE"));
        Assert.True(factory.CreatedConnections.Count >= 2);
    }

    [Fact]
    public async Task TryAcquireAsync_MySql_UsesInsertFirstFallback()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.MySql);
        await using (ctx)
        {
            var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
                "res",
                "owner",
                DateTime.UtcNow.AddMinutes(1),
                DateTime.UtcNow);

            Assert.True(acquired);
            Assert.True(NonQueryContains(factory, "INSERT"));
            Assert.False(NonQueryContains(factory, "MERGE"));
            Assert.False(NonQueryContains(factory, "ON CONFLICT"));
        }
    }

    [Fact]
    public async Task TryAcquireAsync_MySql_WhenInsertHitsUniqueViolation_AttemptsExpiredUpdate()
    {
        var factory = new fakeDbFactory(SupportedDatabase.MySql);
        await using var ctx = new DatabaseContext("Data Source=fake", factory);

        var insertConn = new fakeDbConnection();
        insertConn.SetNonQueryExecuteException(
            new UniqueConstraintViolationException("duplicate", SupportedDatabase.MySql));
        factory.Connections.Insert(0, insertConn);
        var updateConn = new fakeDbConnection();
        updateConn.NonQueryResults.Enqueue(1);
        factory.Connections.Add(updateConn);

        var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
            "res",
            "owner",
            DateTime.UtcNow.AddMinutes(1),
            DateTime.UtcNow);

        Assert.True(acquired);
        Assert.True(factory.CreatedConnections.Count >= 2);
    }

    [Fact]
    public async Task TryAcquireAsync_Firebird_RoutesToInsertFirstFallback()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.Firebird);
        await using (ctx)
        {
            var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
                "res",
                "owner",
                DateTime.UtcNow.AddMinutes(1),
                DateTime.UtcNow);

            Assert.True(acquired);
            Assert.True(NonQueryContains(factory, "INSERT"));
            Assert.False(NonQueryContains(factory, "MERGE"));
        }
    }

    [Fact]
    public async Task TryAcquireAsync_Oracle_UsesOracleMergeSyntax()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.Oracle);
        await using (ctx)
        {
            var acquired = await new DistributedLockGateway(ctx).TryAcquireAsync(
                "res",
                "owner",
                DateTime.UtcNow.AddMinutes(1),
                DateTime.UtcNow);

            Assert.True(acquired);
            var sql = factory.CreatedConnections
                .SelectMany(c => c.ExecutedNonQueryTexts)
                .First(s => s.Contains("MERGE INTO", StringComparison.OrdinalIgnoreCase));
            Assert.Contains("MERGE INTO", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FROM DUAL", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(" WHEN MATCHED THEN UPDATE SET ", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(" WHERE t.", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(" WITH (HOLDLOCK)", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(" AS t", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(";", sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task TryRenewAsync_WhenUpdateAffectsOneRow_ReturnsTrue()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.SqlServer);
        await using (ctx)
        {
            var renewed = await new DistributedLockGateway(ctx).TryRenewAsync(
                "res",
                "owner",
                1,
                DateTime.UtcNow.AddMinutes(1));

            Assert.True(renewed);
            Assert.True(NonQueryContains(factory, "UPDATE"));
            Assert.True(NonQueryContains(factory, "version"));
        }
    }

    [Fact]
    public async Task ReleaseAsync_UsesDeleteWithOwnerGuard()
    {
        var (ctx, factory) = MakeContext(SupportedDatabase.SqlServer);
        await using (ctx)
        {
            await new DistributedLockGateway(ctx).ReleaseAsync("res", "owner");

            Assert.True(NonQueryContains(factory, "DELETE"));
            Assert.True(NonQueryContains(factory, "owner_id"));
        }
    }
}
