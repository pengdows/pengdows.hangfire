namespace pengdows.hangfire;

using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;

public sealed class CountersAggregator : IBackgroundProcess
{
    private const int BatchSize = 1000;
    private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

    private readonly ILog _logger = LogProvider.For<CountersAggregator>();
    private readonly PengdowsCrudJobStorage _storage;
    private readonly TimeSpan _interval;

    public CountersAggregator(PengdowsCrudJobStorage storage, TimeSpan interval)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _interval = interval;
    }

    public void Execute(BackgroundProcessContext context)
    {
        RunCore(
            waitBetweenPasses: delay =>
            {
                context.Wait(delay);
                context.StoppingToken.ThrowIfCancellationRequested();
            });
        context.Wait(_interval);
    }

    internal void RunOnce() => RunCore();

    private void RunCore(Action<TimeSpan>? waitBetweenPasses = null)
    {
        _logger.Debug("Aggregating records in 'Counter' table...");

        int processed;
        do
        {
            processed = _storage.Counters.AggregateAsync(BatchSize).GetAwaiter().GetResult();

            if (processed >= BatchSize)
            {
                waitBetweenPasses?.Invoke(DelayBetweenPasses);
            }
        } while (processed >= BatchSize);

        _logger.Trace("Records from 'Counter' table aggregated.");
    }

    public override string ToString() => nameof(CountersAggregator);
}
