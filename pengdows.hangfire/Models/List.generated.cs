using System;
using System.Data;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models;

[Table("List", "HangFire")]
public partial class List
{
    [Column("Id", DbType.Int64, 1)]
    [Id(writable: false)]
    public long ID { get; set; }

    [Column("Key", DbType.String, 2)]
    [PrimaryKey(1)]
    public string Key { get; set; }

    [Column("Value", DbType.String, 3)]
    public string Value { get; set; }

    [Column("ExpireAt", DbType.DateTime, 4)]
    public DateTime? ExpireAt { get; set; }
}
