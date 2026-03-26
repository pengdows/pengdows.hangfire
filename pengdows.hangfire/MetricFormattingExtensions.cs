using System.Text;
using pengdows.crud.metrics;

namespace pengdows.hangfire;

public static class MetricFormattingExtensions
{
    public static string ToMetricGrid(this DatabaseMetrics m, string? name = null, string? mode = null)
    {
        var sb = new StringBuilder();
        var label = name ?? "Database";
        var modeLabel = mode ?? "Unknown";
        
        sb.AppendLine($"┌────────────────────────────────────────────────────────────────────────────────────────────┐");
        sb.AppendLine($"│ Metrics Grid: {label,-60} Mode: {modeLabel,-13} │");
        sb.AppendLine($"├────────────────────────┬──────────────────────┬──────────────────────┬─────────────────────┤");
        sb.AppendLine($"│ Category / Metric      │ Read Role            │ Write Role           │ Total / Context     │");
        sb.AppendLine($"├────────────────────────┼──────────────────────┼──────────────────────┼─────────────────────┤");
        
        // Connections
        sb.AppendLine($"│ Connections Current    │ {m.Read.ConnectionsCurrent,20} │ {m.Write.ConnectionsCurrent,20} │ {m.ConnectionsCurrent,19} │");
        sb.AppendLine($"│ Connections Peak       │ {m.Read.PeakOpenConnections,20} │ {m.Write.PeakOpenConnections,20} │ {m.PeakOpenConnections,19} │");
        sb.AppendLine($"│ Connections Opened     │ {m.Read.ConnectionsOpened,20} │ {m.Write.ConnectionsOpened,20} │ {m.ConnectionsOpened,19} │");
        
        sb.AppendLine($"├────────────────────────┼──────────────────────┼──────────────────────┼─────────────────────┤");
        
        // Commands
        sb.AppendLine($"│ Commands Executed      │ {m.Read.CommandsExecuted,20} │ {m.Write.CommandsExecuted,20} │ {m.CommandsExecuted,19} │");
        sb.AppendLine($"│ Commands Failed        │ {m.Read.CommandsFailed,20} │ {m.Write.CommandsFailed,20} │ {m.CommandsFailed,19} │");
        sb.AppendLine($"│ Command Avg (ms)       │ {m.Read.AvgCommandMs,20:F2} │ {m.Write.AvgCommandMs,20:F2} │ {m.AvgCommandMs,19:F2} │");
        sb.AppendLine($"│ Command P95 (ms)       │ {m.Read.P95CommandMs,20:F2} │ {m.Write.P95CommandMs,20:F2} │ {m.P95CommandMs,19:F2} │");
        sb.AppendLine($"│ Command P99 (ms)       │ {m.Read.P99CommandMs,20:F2} │ {m.Write.P99CommandMs,20:F2} │ {m.P99CommandMs,19:F2} │");
        
        sb.AppendLine($"├────────────────────────┼──────────────────────┼──────────────────────┼─────────────────────┤");
        
        // Transactions
        sb.AppendLine($"│ Transactions Committed │ {m.Read.TransactionsCommitted,20} │ {m.Write.TransactionsCommitted,20} │ {m.TransactionsCommitted,19} │");
        sb.AppendLine($"│ Transactions RolledBk  │ {m.Read.TransactionsRolledBack,20} │ {m.Write.TransactionsRolledBack,20} │ {m.TransactionsRolledBack,19} │");
        sb.AppendLine($"│ Transaction Avg (ms)   │ {m.Read.AvgTransactionMs,20:F2} │ {m.Write.AvgTransactionMs,20:F2} │ {m.AvgTransactionMs,19:F2} │");
        sb.AppendLine($"│ Transaction P95 (ms)   │ {m.Read.P95TransactionMs,20:F2} │ {m.Write.P95TransactionMs,20:F2} │ {m.P95TransactionMs,19:F2} │");
        sb.AppendLine($"│ Transaction P99 (ms)   │ {m.Read.P99TransactionMs,20:F2} │ {m.Write.P99TransactionMs,20:F2} │ {m.P99TransactionMs,19:F2} │");
        
        sb.AppendLine($"├────────────────────────┼──────────────────────┼──────────────────────┼─────────────────────┤");
        
        // Errors & Misc
        sb.AppendLine($"│ Deadlocks Detected     │ {m.Read.ErrorDeadlocks,20} │ {m.Write.ErrorDeadlocks,20} │ {m.ErrorDeadlocks,19} │");
        sb.AppendLine($"│ Serialization Failures │ {m.Read.ErrorSerializationFailures,20} │ {m.Write.ErrorSerializationFailures,20} │ {m.ErrorSerializationFailures,19} │");
        sb.AppendLine($"│ Constraint Violations  │ {m.Read.ErrorConstraintViolations,20} │ {m.Write.ErrorConstraintViolations,20} │ {m.ErrorConstraintViolations,19} │");
        sb.AppendLine($"│ Slow Commands Total    │ {m.Read.SlowCommandsTotal,20} │ {m.Write.SlowCommandsTotal,20} │ {m.SlowCommandsTotal,19} │");
        sb.AppendLine($"│ Prepared Statemts      │ {m.Read.PreparedStatements,20} │ {m.Write.PreparedStatements,20} │ {m.PreparedStatements,19} │");
        
        sb.AppendLine($"└────────────────────────┴──────────────────────┴──────────────────────┴─────────────────────┘");
        
        return sb.ToString();
    }
}
