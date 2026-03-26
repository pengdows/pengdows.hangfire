using Xunit;

namespace pengdows.hangfire.stress.tests.infrastructure;

[CollectionDefinition("SqlServerStress")]
public sealed class StressCollectionFixture : ICollectionFixture<SqlServerFixture> { }
