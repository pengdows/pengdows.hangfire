namespace pengdows.hangfire.gateways;

using System;
using System.Threading.Tasks;
using pengdows.hangfire.models;

using pengdows.crud;

public interface IDistributedLockGateway : ITableGateway<DistributedLockRecord, string>
{
    /// <summary>
    /// Attempts to acquire the lock for <paramref name="resource"/> in a single UPSERT.
    /// Returns <c>true</c> when the row was inserted (no prior holder) or stolen (prior
    /// holder's TTL had expired); returns <c>false</c> when the lock is actively held
    /// by another owner whose TTL has not yet lapsed.
    /// </summary>
    Task<bool> TryAcquireAsync(string resource, string ownerId, DateTime expiresAt, DateTime asOf);

    Task<bool> TryRenewAsync(string resource, string ownerId, int expectedVersion, DateTime newExpiresAt);
    Task ReleaseAsync(string resource, string ownerId);
}
