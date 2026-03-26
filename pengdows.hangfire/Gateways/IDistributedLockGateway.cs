namespace pengdows.hangfire.gateways;

using System;
using System.Threading.Tasks;
using pengdows.hangfire.models;

using pengdows.crud;

public interface IDistributedLockGateway : ITableGateway<DistributedLockRecord, string>
{
    /// <summary>
    /// Attempts to acquire the lock for <paramref name="resource"/>.
    /// On INSERT success returns <c>(record, false)</c>.
    /// On PK violation, attempts an atomic UPDATE WHERE expires_at &lt;= asOf:
    ///   if 1 row updated returns <c>(record, true)</c> (steal);
    ///   if 0 rows updated returns <c>(null, false)</c> (live row held by another owner).
    /// </summary>
    Task<(DistributedLockRecord? Record, bool WasSteal)> TryAcquireAsync(
        string resource, string ownerId, DateTime expiresAt, DateTime asOf);

    Task<bool> TryRenewAsync(string resource, string ownerId, int expectedVersion, DateTime newExpiresAt);
    Task ReleaseAsync(string resource, string ownerId);
}
