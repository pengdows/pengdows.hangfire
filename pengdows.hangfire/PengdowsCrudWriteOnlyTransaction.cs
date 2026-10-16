namespace pengdows.hangfire;

using System;
using System.Collections.Generic;
using System.Linq;
using pengdows.crud.enums;
using Hangfire.States;
using Hangfire.Storage;
using pengdows.hangfire.contracts;
using pengdows.hangfire.models;
using pengdows.crud;
using pengdows.crud.exceptions;

public sealed class PengdowsCrudWriteOnlyTransaction : JobStorageTransaction, IHangfireTransaction
{
    private readonly PengdowsCrudJobStorage _storage;
    private readonly List<Func<IDatabaseContext, Task>> _commands = new();
    private readonly List<IDisposable> _acquiredLocks = new();

    public PengdowsCrudWriteOnlyTransaction(PengdowsCrudJobStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public override void Commit()
    {
        CommitAsync().GetAwaiter().GetResult();
    }

    public async Task CommitAsync()
    {
        var isolation = _storage.DatabaseContext.Product == pengdows.crud.enums.SupportedDatabase.PostgreSql
            ? pengdows.crud.enums.IsolationProfile.StrictConsistency
            : pengdows.crud.enums.IsolationProfile.SafeNonBlockingReads;
        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            await using var tx = await _storage.DatabaseContext.BeginTransactionAsync(isolation);
            try
            {
                foreach (var command in _commands)
                {
                    await command(tx);
                }
                await tx.CommitAsync();
                return;
            }
            catch (SerializationConflictException) when (attempt < maxAttempts)
            {
                try { await tx.RollbackAsync(); } catch { }
                await Task.Delay(TimeSpan.FromMilliseconds(25 * attempt));
            }
            catch
            {
                try { await tx.RollbackAsync(); } catch { }
                throw;
            }
        }
    }

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _commands.Add(async tx =>
            await _storage.Jobs.UpdateExpireAtAsync(id, _storage.Clock.UtcNow.Add(expireIn), tx));
    }

    public override void PersistJob(string jobId)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _commands.Add(async tx =>
            await _storage.Jobs.UpdateExpireAtAsync(id, null, tx));
    }

    public override void SetJobState(string jobId, IState state)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _commands.Add(async tx =>
        {
            await _storage.Jobs.UpdateStateNameAsync(id, state.Name, tx);
            await _storage.JobStates.CreateAsync(new State
            {
                JobID     = id,
                Name      = state.Name,
                Reason    = state.Reason,
                CreatedAt = _storage.Clock.UtcNow,
                Data      = JsonHelper.Serialize(state.SerializeData())
            }, tx);
        });
    }

    public override void AddJobState(string jobId, IState state)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _commands.Add(async tx =>
        {
            await _storage.JobStates.CreateAsync(new State
            {
                JobID     = id,
                Name      = state.Name,
                Reason    = state.Reason,
                CreatedAt = _storage.Clock.UtcNow,
                Data      = JsonHelper.Serialize(state.SerializeData())
            }, tx);
        });
    }

    public override void AddToQueue(string queue, string jobId)
    {
        if (!long.TryParse(jobId, out var id))
        {
            return;
        }

        _commands.Add(async tx =>
        {
            await _storage.JobQueues.CreateAsync(new JobQueue { Queue = queue, JobID = id }, tx);
        });
    }

    public override void IncrementCounter(string key)
        => _commands.Add(async tx => await _storage.Counters.AppendAsync(key, 1, null, tx));

    public override void IncrementCounter(string key, TimeSpan expireIn)
        => _commands.Add(async tx => await _storage.Counters.AppendAsync(key, 1, _storage.Clock.UtcNow.Add(expireIn), tx));

    public override void DecrementCounter(string key)
        => _commands.Add(async tx => await _storage.Counters.AppendAsync(key, -1, null, tx));

    public override void DecrementCounter(string key, TimeSpan expireIn)
        => _commands.Add(async tx => await _storage.Counters.AppendAsync(key, -1, _storage.Clock.UtcNow.Add(expireIn), tx));

    public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

    public override void AddToSet(string key, string value, double score)
    {
        _commands.Add(async tx =>
        {
            await _storage.Sets.UpsertAsync(new Set { Key = key, Value = value, Score = score }, tx);
        });
    }

    public override void RemoveFromSet(string key, string value)
    {
        _commands.Add(async tx =>
        {
            await _storage.Sets.BatchDeleteAsync(new[] { new Set { Key = key, Value = value } }, tx);
        });
    }

    public override void InsertToList(string key, string value)
        => _commands.Add(async tx => await _storage.Lists.AppendAsync(key, value, tx));

    public override void RemoveFromList(string key, string value)
        => _commands.Add(async tx => await _storage.Lists.DeleteByKeyValueAsync(key, value, tx));

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        => _commands.Add(async tx => await _storage.Lists.TrimAsync(key, keepStartingFrom, keepEndingAt, tx));

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (keyValuePairs == null)
        {
            throw new ArgumentNullException(nameof(keyValuePairs));
        }

        var entities = keyValuePairs.Select(p => new Hash { Key = key, Field = p.Key, Value = p.Value }).ToList();
        _commands.Add(async tx => await _storage.Hashes.UpsertAsync(entities, tx));
    }

    public override void RemoveHash(string key)
        => _commands.Add(async tx => await _storage.Hashes.DeleteAllForKeyAsync(key, tx));

    public override void ExpireSet(string key, TimeSpan expireIn)
        => _commands.Add(async tx => await _storage.Sets.UpdateExpireAtAsync(key, _storage.Clock.UtcNow.Add(expireIn), tx));

    public override void PersistSet(string key)
        => _commands.Add(async tx => await _storage.Sets.UpdateExpireAtAsync(key, null, tx));

    public override void ExpireHash(string key, TimeSpan expireIn)
        => _commands.Add(async tx => await _storage.Hashes.UpdateExpireAtAsync(key, _storage.Clock.UtcNow.Add(expireIn), tx));

    public override void PersistHash(string key)
        => _commands.Add(async tx => await _storage.Hashes.UpdateExpireAtAsync(key, null, tx));

    public override void ExpireList(string key, TimeSpan expireIn)
        => _commands.Add(async tx => await _storage.Lists.UpdateExpireAtAsync(key, _storage.Clock.UtcNow.Add(expireIn), tx));

    public override void PersistList(string key)
        => _commands.Add(async tx => await _storage.Lists.UpdateExpireAtAsync(key, null, tx));

    public override void AddRangeToSet(string key, IList<string> items)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        var entities = items.Select(v => new Set { Key = key, Value = v, Score = 0.0 }).ToList();
        _commands.Add(async tx => await _storage.Sets.UpsertAsync(entities, tx));
    }

    public override void RemoveSet(string key)
        => _commands.Add(async tx => await _storage.Sets.DeleteByKeyAsync(key, tx));

    public override void AcquireDistributedLock(string resource, TimeSpan timeout)
        => _acquiredLocks.Add(new PengdowsCrudDistributedLock(_storage, resource, timeout));

    public override void RemoveFromQueue(IFetchedJob fetchedJob)
    {
        if (fetchedJob is PengdowsCrudFetchedJob job)
        {
            _commands.Add(_ =>
            {
                job.RemoveFromQueue();
                return Task.CompletedTask;
            });
        }
    }

    public override void Dispose()
    {
        foreach (var l in _acquiredLocks)
        {
            l.Dispose();
        }

        _acquiredLocks.Clear();
        base.Dispose();
    }
}
