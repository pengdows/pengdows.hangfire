namespace pengdows.hangfire.models;

using System;
using System.Data;
using pengdows.crud.attributes;

[Table("hf_lock", "HangFire")]
public sealed class DistributedLockRecord
{
    [Id]
    [Column("resource", DbType.String, 1)]
    public string Resource { get; set; } = null!;

    [Column("owner_id", DbType.String, 2)]
    public string OwnerId { get; set; } = null!;

    [Column("expires_at", DbType.DateTime, 3)]
    public DateTime ExpiresAt { get; set; }

    [Version]
    [Column("version", DbType.Int32, 4)]
    public int Version { get; set; }
}
