using System;
using System.Linq;
using System.Threading.Tasks;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using pengdows.hangfire.gateways;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class DistributedLockGatewayContextTests
{
    [Fact]
    public async Task TryAcquireAsync_UsesSuppliedContextForExecution()
    {
        var rootFactory = new fakeDbFactory(SupportedDatabase.SqlServer);
        await using var rootContext = new DatabaseContext("Data Source=root", rootFactory);

        var overrideFactory = new fakeDbFactory(SupportedDatabase.SqlServer);
        await using var overrideContext = new DatabaseContext("Data Source=override", overrideFactory);

        var gateway = new DistributedLockGateway(rootContext);

        var acquired = await gateway.TryAcquireAsync(
            "ctx-lock",
            "owner-1",
            DateTime.UtcNow.AddMinutes(1),
            DateTime.UtcNow,
            overrideContext);

        Assert.True(acquired);
        Assert.Contains(
            overrideFactory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts),
            sql => sql.Contains("MERGE", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(
            rootFactory.CreatedConnections.SelectMany(c => c.ExecutedNonQueryTexts),
            sql => sql.Contains("MERGE", StringComparison.OrdinalIgnoreCase));
    }
}
