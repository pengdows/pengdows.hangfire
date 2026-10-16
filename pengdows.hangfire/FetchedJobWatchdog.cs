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
        var cutoff = _storage.Clock.UtcNow - _storage.Options.InvisibilityTimeout;
        try
        {
            try
            {
                var unfenced = _storage.JobQueues.CountUnfencedFetchedAsync().GetAwaiter().GetResult();
                if (unfenced > 0)
                {
                    Logger.WarnFormat("Detected {0} fetched job(s) without a fetch token; a pre-2.0.6 server is fetching jobs from this storage; upgrade all servers.", unfenced);
                }
            }
            catch (Exception ex)
            {
                Logger.DebugException("Unable to inspect for pre-2.0.6 fetched jobs.", ex);
            }
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
