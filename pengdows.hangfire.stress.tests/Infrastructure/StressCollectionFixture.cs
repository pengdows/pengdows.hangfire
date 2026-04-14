using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

[CollectionDefinition("SqlServerStress")]
public sealed class SqlServerStressCollectionFixture : ICollectionFixture<SqlServerFixture> { }

[CollectionDefinition("PostgresStress")]
public sealed class PostgresStressCollectionFixture : ICollectionFixture<PostgresStressFixture> { }
