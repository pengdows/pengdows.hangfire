using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "Set")]
public partial class Set
{
    [Column("Key", DbType.String, 2)]
    [PrimaryKey(1)]
    public string Key { get; set; }

    [Column("Score", DbType.Double, 3)]
    public double Score { get; set; }

    [Column("Value", DbType.String, 4)]
    [PrimaryKey(2)]
    public string Value { get; set; }

    [Column("ExpireAt", DbType.DateTime, 5)]
    public DateTime? ExpireAt { get; set; }
}
