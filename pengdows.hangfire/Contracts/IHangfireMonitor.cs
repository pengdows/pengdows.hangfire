using Hangfire.Storage;
using pengdows.crud.metrics;

namespace pengdows.hangfire.contracts;

/// <summary>
/// Our interface seam over IMonitoringApi.
/// If Hangfire adds or removes a member from IMonitoringApi, this interface
/// will fail to compile, catching the break before it reaches the concrete class.
/// </summary>
public interface IHangfireMonitor : IMonitoringApi
{
    /// <summary>
    /// Retrieves a snapshot of internal database metrics from pengdows.crud.
    /// Keyed by database/context name.
    /// </summary>
    IDictionary<string, DatabaseMetrics> GetDatabaseMetrics();

    /// <summary>
    /// Returns a pre-formatted ASCII grid of database metrics for each active database.
    /// </summary>
    string GetDatabaseMetricGrid();
}
