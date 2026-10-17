using Xunit;

namespace pengdows.hangfire.integration.tests;

[Collection("PostgreSql")]
public sealed class PostgresConfigurationFacts
{
    private readonly PostgresFixture _fixture;

    public PostgresConfigurationFacts(PostgresFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ServerReportsConfiguredMaxConnections()
    {
        var maxConnections = await _fixture.QueryScalarAsync<int>("SHOW max_connections");

        Assert.Equal(200, maxConnections);
    }
}
