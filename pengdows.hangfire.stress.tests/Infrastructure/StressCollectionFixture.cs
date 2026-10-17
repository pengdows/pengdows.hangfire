using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

[CollectionDefinition("SqlServerStress")]
public sealed class SqlServerStressCollectionFixture : ICollectionFixture<SqlServerFixture> { }

[CollectionDefinition("PostgresStress")]
public sealed class PostgresStressCollectionFixture : ICollectionFixture<PostgresStressFixture> { }

[CollectionDefinition("MySqlStress")]
public sealed class MySqlStressCollectionFixture : ICollectionFixture<MySqlStressFixture> { }

[CollectionDefinition("MariaDbStress")]
public sealed class MariaDbStressCollectionFixture : ICollectionFixture<MariaDbStressFixture> { }
