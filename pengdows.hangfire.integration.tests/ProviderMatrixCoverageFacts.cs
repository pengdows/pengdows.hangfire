using System;
using System.Linq;
using Xunit;

namespace pengdows.hangfire.integration.tests;

/// <summary>
/// Guards the live provider matrix. Every provider must have a concrete class
/// for each provider-neutral integration contract and the provider stress suite.
/// Provider-specific tests (for example SQLite locking behavior) are intentionally
/// excluded from this matrix.
/// </summary>
public sealed class ProviderMatrixCoverageFacts
{
    private static readonly string[] Providers =
    [
        "Sqlite", "Postgres", "SqlServer", "MySql", "Oracle", "Firebird",
        "CockroachDb", "MariaDb", "DuckDb", "YugabyteDb", "TiDb"
    ];

    private static readonly string[] ContractSuites =
    [
        "ConnectionFacts",
        "CountersAggregatorFacts",
        "DistributedLockFacts",
        "ExpirationManagerFacts",
        "FetchedJobWatchdogFacts",
        "JobQueueFacts",
        "WriteOnlyTransactionFacts",
        "ProviderQueueStressFacts"
    ];

    [Fact]
    public void EveryClaimedProviderHasEveryProviderNeutralContractSuite()
    {
        var assembly = typeof(ProviderMatrixCoverageFacts).Assembly;
        var missing =
            from provider in Providers
            from suite in ContractSuites
            let typeName = $"pengdows.hangfire.integration.tests.{provider}{suite}"
            where assembly.GetType(typeName) is null
            select typeName;

        var missingTypes = missing.ToArray();
        Assert.True(missingTypes.Length == 0,
            $"Missing provider contract implementations: {string.Join(", ", missingTypes)}");
    }
}
