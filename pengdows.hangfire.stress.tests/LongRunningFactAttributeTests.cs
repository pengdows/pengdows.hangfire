using System;
using Xunit;

namespace pengdows.hangfire.stress.tests;

public sealed class LongRunningFactAttributeTests
{
    [Fact]
    public void LongRunningFact_SkipsWithoutOptIn()
    {
        WithEnvironmentVariable(null, () =>
        {
            var attribute = new LongRunningFactAttribute();

            Assert.NotNull(attribute.Skip);
        });
    }

    [Fact]
    public void LongRunningFact_RunsWithOptIn()
    {
        WithEnvironmentVariable("1", () =>
        {
            var attribute = new LongRunningFactAttribute();

            Assert.Null(attribute.Skip);
        });
    }

    private static void WithEnvironmentVariable(string? value, Action action)
    {
        const string name = "PENGDOWS_RUN_LONG_RUNNING";
        var original = Environment.GetEnvironmentVariable(name);
        try
        {
            Environment.SetEnvironmentVariable(name, value);
            action();
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, original);
        }
    }
}
