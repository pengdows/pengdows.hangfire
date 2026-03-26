using System;
using System.Linq;
using System.Threading.Tasks;
using pengdows.hangfire.gateways;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Verifies that DeleteExpiredAsync uses a database-independent SELECT-then-DELETE
/// pattern on every gateway. No dialect-specific SQL (DELETE TOP, ctid, LIMIT) may
/// appear in any DELETE statement; batching is delegated to AppendPaging and
/// BatchDeleteAsync which are dialect-aware internally.
/// </summary>
public sealed class DeleteExpiredSqlTests
{
    // Run each gateway test against all four representative databases to confirm
    // the implementation is dialect-neutral.
    public static TheoryData<SupportedDatabase> AllDialects => new()
    {
        SupportedDatabase.SqlServer,
        SupportedDatabase.PostgreSql,
        SupportedDatabase.MySql,
        SupportedDatabase.Sqlite,
    };

    // ── helpers ──────────────────────────────────────────────────────────────

    private static (DatabaseContext Context, fakeDbFactory Factory) MakeContext(SupportedDatabase db)
    {
        var factory = new fakeDbFactory(db);
        return (new DatabaseContext("Data Source=fake", factory), factory);
    }

    private static bool AnyNonQueryContains(fakeDbFactory factory, string fragment) =>
        factory.CreatedConnections
            .SelectMany(c => c.ExecutedNonQueryTexts)
            .Any(s => s.Contains(fragment, StringComparison.OrdinalIgnoreCase));

    private static bool AnyReaderContains(fakeDbFactory factory, string fragment) =>
        factory.CreatedConnections
            .SelectMany(c => c.ExecutedReaderTexts)
            .Any(s => s.Contains(fragment, StringComparison.OrdinalIgnoreCase));

    // ── no rows → early return, no DELETE issued ──────────────────────────────

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Hash_DeleteExpired_NoRows_Returns0(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            var result = await new HashGateway(context).DeleteExpiredAsync(100);
            Assert.Equal(0, result);
            Assert.False(AnyNonQueryContains(factory, "DELETE"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Set_DeleteExpired_NoRows_Returns0(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            var result = await new SetGateway(context).DeleteExpiredAsync(100);
            Assert.Equal(0, result);
            Assert.False(AnyNonQueryContains(factory, "DELETE"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task List_DeleteExpired_NoRows_Returns0(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            var result = await new ListGateway(context).DeleteExpiredAsync(100);
            Assert.Equal(0, result);
            Assert.False(AnyNonQueryContains(factory, "DELETE"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Job_DeleteExpired_NoRows_Returns0(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            var result = await new JobGateway(context).DeleteExpiredAsync(100);
            Assert.Equal(0, result);
            Assert.False(AnyNonQueryContains(factory, "DELETE"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task AggregatedCounter_DeleteExpired_NoRows_Returns0(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            var result = await new AggregatedCounterGateway(context).DeleteExpiredAsync(100);
            Assert.Equal(0, result);
            Assert.False(AnyNonQueryContains(factory, "DELETE"));
        }
    }

    // ── SELECT step is issued and contains no hardcoded dialect syntax ────────

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Hash_DeleteExpired_SelectStep_NoDialectSpecificSyntax(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            await new HashGateway(context).DeleteExpiredAsync(100);
            Assert.True(AnyReaderContains(factory, "ExpireAt"));
            Assert.False(AnyReaderContains(factory, "ctid"));
            Assert.False(AnyReaderContains(factory, "DELETE TOP"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Set_DeleteExpired_SelectStep_NoDialectSpecificSyntax(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            await new SetGateway(context).DeleteExpiredAsync(100);
            Assert.True(AnyReaderContains(factory, "ExpireAt"));
            Assert.False(AnyReaderContains(factory, "ctid"));
            Assert.False(AnyReaderContains(factory, "DELETE TOP"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task List_DeleteExpired_SelectStep_NoDialectSpecificSyntax(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            await new ListGateway(context).DeleteExpiredAsync(100);
            Assert.True(AnyReaderContains(factory, "ExpireAt"));
            Assert.False(AnyReaderContains(factory, "DELETE TOP"));
        }
    }

    [Theory, MemberData(nameof(AllDialects))]
    public async Task Job_DeleteExpired_SelectStep_NoDialectSpecificSyntax(SupportedDatabase db)
    {
        var (context, factory) = MakeContext(db);
        await using (context)
        {
            await new JobGateway(context).DeleteExpiredAsync(100);
            Assert.True(AnyReaderContains(factory, "ExpireAt"));
            Assert.False(AnyReaderContains(factory, "DELETE TOP"));
        }
    }
}
