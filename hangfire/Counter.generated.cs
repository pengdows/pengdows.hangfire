using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "Counter")]
public partial class Counter
{
    [Column("Key", DbType.String, 2)]
    [PrimaryKey(1)]
    public string Key { get; set; }

    [Column("Value", DbType.Int32, 3)]
    public int Value { get; set; }

    [Column("ExpireAt", DbType.DateTime, 4)]
    public DateTime? ExpireAt { get; set; }

    [Column("Id", DbType.Int64, 5)]
    [Id(writable: false)]
    public long ID { get; set; }
}
