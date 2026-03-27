namespace pengdows.hangfire;

using System;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

public sealed class ExpirationManager : IBackgroundProcess
{
    private const string DistributedLockKey = "locks:expirationmanager";
    private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);
    private const int DefaultBatchSize = 1000;

    private readonly ILog _logger = LogProvider.For<ExpirationManager>();
    private readonly PengdowsCrudJobStorage _storage;
    private readonly TimeSpan _checkInterval;

    public ExpirationManager(PengdowsCrudJobStorage storage, TimeSpan checkInterval)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _checkInterval = checkInterval;
    }

    public void Execute(BackgroundProcessContext context)
    {
        try
        {
            using var distributedLock = new PengdowsCrudDistributedLock(_storage, DistributedLockKey, DefaultLockTimeout);

            var batchSize = DefaultBatchSize;

            DeleteExpiredRows("AggregatedCounter", () => _storage.AggregatedCounters.DeleteExpiredAsync(batchSize).GetAwaiter().GetResult());
            DeleteExpiredRows("Job",               () => _storage.Jobs.DeleteExpiredAsync(batchSize).GetAwaiter().GetResult());
            DeleteExpiredRows("List",              () => _storage.Lists.DeleteExpiredAsync(batchSize).GetAwaiter().GetResult());
            DeleteExpiredRows("Set",               () => _storage.Sets.DeleteExpiredAsync(batchSize).GetAwaiter().GetResult());
            DeleteExpiredRows("Hash",              () => _storage.Hashes.DeleteExpiredAsync(batchSize).GetAwaiter().GetResult());
        }
        catch (DistributedLockTimeoutException e) when (e.Resource == DistributedLockKey)
        {
            _logger.Log(LogLevel.Debug,
                () => $"Could not acquire lock on '{DistributedLockKey}' within {DefaultLockTimeout.TotalSeconds}s. Another server handled expiration.",
                e);
        }

        context.Wait(_checkInterval);
    }

    private void DeleteExpiredRows(string table, Func<int> deleteAction)
    {
        _logger.Debug($"Removing expired records from '{table}'...");
        try
        {
            int affected;
            do
            {
                affected = deleteAction();
            } while (affected >= DefaultBatchSize);
        }
        catch (Exception ex)
        {
            _logger.ErrorException($"Error cleaning up '{table}': {ex.Message}", ex);
        }
        _logger.Trace($"Finished removing expired records from '{table}'.");
    }

    public override string ToString() => nameof(ExpirationManager);
}
