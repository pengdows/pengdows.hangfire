using System.Collections.Concurrent;
using pengdows.hangfire.models;
using pengdows.hangfire.stress.tests.infrastructure;
using Xunit;

namespace pengdows.hangfire.stress.tests;

[Collection("SqlServerStress")]
public sealed class JobQueueStressFacts
{
    private readonly SqlServerFixture _fixture;

    public JobQueueStressFacts(SqlServerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(Timeout = 120_000)]
    public async Task ConcurrentWorkers_ClaimEachQueuedJobExactlyOnce()
    {
        const int jobCount = 100;
        const int workerCount = 20;
        const string queue = "stress-queue-claims";

        var claimed = new ConcurrentDictionary<long, byte>();

        for (var i = 0; i < jobCount; i++)
        {
            var job = new Job
            {
                InvocationData = "{}",
                Arguments = "[]",
                CreatedAt = DateTime.UtcNow
            };
            await _fixture.Storage.Jobs.CreateAsync(job);
            await _fixture.Storage.JobQueues.CreateAsync(new JobQueue
            {
                JobID = job.ID,
                Queue = queue
            });
        }

        var workers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
        {
            var idlePolls = 0;
            while (idlePolls < 20)
            {
                var result = await _fixture.Storage.JobQueues.FetchNextJobAsync(
                    [queue], CancellationToken.None);
                if (result is null)
                {
                    idlePolls++;
                    await Task.Delay(10);
                    continue;
                }

                idlePolls = 0;
                Assert.True(claimed.TryAdd(result.Value.JobId, 0),
                    $"Job {result.Value.JobId} was claimed more than once.");
                Assert.Equal(1, await _fixture.Storage.JobQueues.AcknowledgeAsync(
                    result.Value.JobId, result.Value.Queue));
            }
        })).ToArray();

        await Task.WhenAll(workers);

        Assert.Equal(jobCount, claimed.Count);
        var remaining = await _fixture.Storage.JobQueues.GetPagedByQueueAsync(
            queue, 0, jobCount, fetched: false);
        Assert.Empty(remaining);
    }

    [Fact(Timeout = 120_000)]
    public async Task ConcurrentProducersAndConsumers_DrainQueueWithoutLoss()
    {
        const int producerCount = 4;
        const int jobsPerProducer = 25;
        const int workerCount = 12;
        const int expectedJobs = producerCount * jobsPerProducer;
        const string queue = "stress-queue-producer-consumer";

        var claimed = new ConcurrentDictionary<long, byte>();
        var producersCompleted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var producers = Enumerable.Range(0, producerCount).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < jobsPerProducer; i++)
            {
                var job = new Job
                {
                    InvocationData = "{}",
                    Arguments = "[]",
                    CreatedAt = DateTime.UtcNow
                };
                await _fixture.Storage.Jobs.CreateAsync(job);
                await _fixture.Storage.JobQueues.CreateAsync(new JobQueue
                {
                    JobID = job.ID,
                    Queue = queue
                });
            }
        })).ToArray();

        var producerCoordinator = Task.Run(async () =>
        {
            try
            {
                await Task.WhenAll(producers);
                producersCompleted.SetResult();
            }
            catch (Exception ex)
            {
                producersCompleted.SetException(ex);
                throw;
            }
        });

        var consumers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
        {
            var idlePolls = 0;
            while (!producersCompleted.Task.IsCompleted || claimed.Count < expectedJobs)
            {
                var result = await _fixture.Storage.JobQueues.FetchNextJobAsync(
                    [queue], CancellationToken.None);
                if (result is null)
                {
                    idlePolls++;
                    if (producersCompleted.Task.IsCompleted && idlePolls >= 50)
                    {
                        break;
                    }

                    await Task.Delay(10);
                    continue;
                }

                idlePolls = 0;
                Assert.True(claimed.TryAdd(result.Value.JobId, 0),
                    $"Job {result.Value.JobId} was claimed more than once.");
                Assert.Equal(1, await _fixture.Storage.JobQueues.AcknowledgeAsync(
                    result.Value.JobId, result.Value.Queue));
            }
        })).ToArray();

        await Task.WhenAll(consumers.Append(producerCoordinator));

        Assert.Equal(expectedJobs, claimed.Count);
        var remaining = await _fixture.Storage.JobQueues.GetPagedByQueueAsync(
            queue, 0, expectedJobs, fetched: false);
        Assert.Empty(remaining);
    }
}
