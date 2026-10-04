using System;
using Xunit;

namespace pengdows.hangfire.stress.tests;

internal sealed class LongRunningFactAttribute : FactAttribute
{
    public LongRunningFactAttribute()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("PENGDOWS_RUN_LONG_RUNNING"),
                "1",
                StringComparison.Ordinal))
        {
            Skip = "Long-running stress tests are opt-in; set PENGDOWS_RUN_LONG_RUNNING=1.";
        }
    }
}
