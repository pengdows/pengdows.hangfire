namespace pengdows.hangfire;

using System;
using Hangfire.Logging;
using Hangfire.Server;

public sealed class FetchedJobWatchdog : IBackgroundProcess
{
    private static readonly ILog Logger = LogProvider.For<FetchedJobWatchdog>();

    private readonly PengdowsCrudJobStorage _storage;
    private readonly TimeSpan _checkInterval;

    public FetchedJobWatchdog(PengdowsCrudJobStorage storage, TimeSpan checkInterval)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _checkInterval = checkInterval;
    }

    public void Execute(BackgroundProcessContext context)
    {
        RunOnce();
        context.Wait(_checkInterval);
    }

    internal void RunOnce()
    {
        var cutoff = DateTime.UtcNow - _storage.Options.InvisibilityTimeout;
        try
        {
            var requeued = _storage.JobQueues.RequeueStaleAsync(cutoff).GetAwaiter().GetResult();
            if (requeued > 0)
            {
                Logger.InfoFormat("Requeued {0} orphaned job(s) with FetchedAt <= {1:u}.", requeued, cutoff);
            }
        }
        catch (Exception ex)
        {
            Logger.ErrorException("Error requeuing orphaned fetched jobs.", ex);
        }
    }

    public override string ToString() => nameof(FetchedJobWatchdog);
}
