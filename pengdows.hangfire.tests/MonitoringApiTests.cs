using System.Reflection;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

/// <summary>
/// Unit tests for PengdowsCrudMonitoringApi covering:
/// - TruncateToHour correctness (private static method, tested via reflection)
/// - GetTimeline slot counts and zero-default when no AggregatedCounter rows exist
/// - Empty-queue early return in EnqueuedJobs / FetchedJobs
/// </summary>
public sealed class MonitoringApiTests
{
    private static (PengdowsCrudMonitoringApi Api, fakeDbFactory Factory) CreateApi()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var context = new DatabaseContext("Data Source=fake;EmulatedProduct=SqlServer", factory);
        var storage = new PengdowsCrudJobStorage(context);
        return (new PengdowsCrudMonitoringApi(storage), factory);
    }

    // ── TruncateToHour ───────────────────────────────────────────────────────

    private static readonly MethodInfo _truncateToHour =
        typeof(PengdowsCrudMonitoringApi)
            .GetMethod("TruncateToHour", BindingFlags.NonPublic | BindingFlags.Static)!;

    [Fact]
    public void TruncateToHour_Drops_MinutesSecondsAndMilliseconds()
    {
        var input  = new DateTime(2024, 6, 15, 14, 37, 22, 500, DateTimeKind.Utc);
        var result = (DateTime)_truncateToHour.Invoke(null, [input])!;

        Assert.Equal(new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void TruncateToHour_AlreadyOnHourBoundary_IsUnchanged()
    {
        var input  = new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Utc);
        var result = (DateTime)_truncateToHour.Invoke(null, [input])!;

        Assert.Equal(input, result);
    }

    [Fact]
    public void TruncateToHour_Preserves_UtcKind()
    {
        var input  = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var result = (DateTime)_truncateToHour.Invoke(null, [input])!;

        Assert.Equal(DateTimeKind.Utc, result.Kind);
    }

    // ── GetTimeline slot counts ───────────────────────────────────────────────

    [Fact]
    public void SucceededByDatesCount_Returns_Exactly_7_Slots()
    {
        var (api, _) = CreateApi();

        var result = api.SucceededByDatesCount();

        Assert.Equal(7, result.Count);
    }

    [Fact]
    public void FailedByDatesCount_Returns_Exactly_7_Slots()
    {
        var (api, _) = CreateApi();

        var result = api.FailedByDatesCount();

        Assert.Equal(7, result.Count);
    }

    [Fact]
    public void HourlySucceededJobs_Returns_Exactly_24_Slots()
    {
        var (api, _) = CreateApi();

        var result = api.HourlySucceededJobs();

        Assert.Equal(24, result.Count);
    }

    [Fact]
    public void HourlyFailedJobs_Returns_Exactly_24_Slots()
    {
        var (api, _) = CreateApi();

        var result = api.HourlyFailedJobs();

        Assert.Equal(24, result.Count);
    }

    // ── GetTimeline zero-defaults when no rows ────────────────────────────────

    [Fact]
    public void SucceededByDatesCount_AllSlotsZero_WhenNoAggregatedCounterRows()
    {
        var (api, _) = CreateApi();

        var result = api.SucceededByDatesCount();

        Assert.All(result, kvp => Assert.Equal(0L, kvp.Value));
    }

    [Fact]
    public void HourlySucceededJobs_AllSlotsZero_WhenNoAggregatedCounterRows()
    {
        var (api, _) = CreateApi();

        var result = api.HourlySucceededJobs();

        Assert.All(result, kvp => Assert.Equal(0L, kvp.Value));
    }

    // ── Daily slots are date-only keys (no time component) ───────────────────

    [Fact]
    public void SucceededByDatesCount_Keys_Are_Midnight_Utc()
    {
        var (api, _) = CreateApi();

        var result = api.SucceededByDatesCount();

        foreach (var key in result.Keys)
        {
            Assert.Equal(TimeSpan.Zero, key.TimeOfDay);
            Assert.Equal(DateTimeKind.Utc, key.Kind);
        }
    }

    // ── Hourly slots have no sub-hour component ───────────────────────────────

    [Fact]
    public void HourlySucceededJobs_Keys_Have_No_Minutes_Or_Seconds()
    {
        var (api, _) = CreateApi();

        var result = api.HourlySucceededJobs();

        foreach (var key in result.Keys)
        {
            Assert.Equal(0, key.Minute);
            Assert.Equal(0, key.Second);
            Assert.Equal(0, key.Millisecond);
        }
    }

    // ── Empty-queue early returns ─────────────────────────────────────────────

    [Fact]
    public void EnqueuedJobs_EmptyQueue_ReturnsEmptyList()
    {
        var (api, _) = CreateApi();

        var result = api.EnqueuedJobs("default", 0, 10);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void FetchedJobs_EmptyQueue_ReturnsEmptyList()
    {
        var (api, _) = CreateApi();

        var result = api.FetchedJobs("default", 0, 10);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void EnqueuedJobs_EmptyQueue_DoesNotQueryJobTable()
    {
        var (api, factory) = CreateApi();

        api.EnqueuedJobs("default", 0, 10);

        // When the queue is empty, EnqueuedJobs returns immediately without
        // fetching individual jobs. No Job-table SELECT should appear.
        var readerTexts = factory.CreatedConnections
            .SelectMany(c => c.ExecutedReaderTexts)
            .Where(s => s.Contains("Job", StringComparison.OrdinalIgnoreCase)
                     && s.Contains("Id", StringComparison.OrdinalIgnoreCase)
                     && !s.Contains("Queue", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.Empty(readerTexts);
    }
}
