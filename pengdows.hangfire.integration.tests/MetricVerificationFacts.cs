using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace pengdows.hangfire.integration.tests;

[Collection("Sqlite")]
public class MetricVerificationFacts
{
    private readonly SqliteFixture _f;
    private readonly ITestOutputHelper _out;

    public MetricVerificationFacts(SqliteFixture fixture, ITestOutputHelper output)
    {
        _f = fixture;
        _out = output;
    }

    [Fact]
    public async Task ReadOperations_ShowInMetricsGrid()
    {
        // 1. Ensure we have some data
        var jobId = await _f.InsertJobAsync();
        
        // 2. Perform some READ operations via Monitoring API
        var monitor = (PengdowsCrudMonitoringApi)_f.Storage.GetMonitoringApi();
        
        // This performs several READ queries (CountAll, CountWhere, etc.)
        _out.WriteLine("Calling GetStatistics (READ)...");
        var stats = monitor.GetStatistics();
        
        // This performs another READ query
        _out.WriteLine("Calling JobDetails (READ)...");
        var jobData = monitor.JobDetails(jobId.ToString());
        
        // 3. Get the grid
        var grid = monitor.GetDatabaseMetricGrid();
        _out.WriteLine(grid);
        
        // 4. Verify Read Role is NOT zero
        Assert.Contains("Read Role", grid);
        
        var lines = grid.Split(Environment.NewLine);
        bool foundReadCount = false;
        foreach (var line in lines)
        {
            if (line.Contains("Commands Executed"))
            {
                var parts = line.Split('│', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (parts.Length >= 2)
                {
                    var readCount = long.Parse(parts[1]);
                    _out.WriteLine($"Verified Read Role Commands Executed: {readCount}");
                    if (readCount > 0)
                    {
                        foundReadCount = true;
                    }
                }
            }
        }
        
        Assert.True(foundReadCount, "Read Role Commands Executed should be greater than 0 after monitor calls.");
    }
}
