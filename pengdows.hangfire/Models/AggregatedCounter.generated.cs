using System;
using System.Data;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models;

[Table("AggregatedCounter", "HangFire")]
public partial class AggregatedCounter
{
    [Column("Key", DbType.String, 2)]
    [Id]
    public string Key { get; set; }

    [Column("Value", DbType.Int64, 3)]
    public long Value { get; set; }

    [Column("ExpireAt", DbType.DateTime, 4)]
    public DateTime? ExpireAt { get; set; }
}
