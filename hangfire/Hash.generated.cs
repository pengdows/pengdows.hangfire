using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "Hash")]
public partial class Hash
{
    [Column("Key", DbType.String, 2)]
    [PrimaryKey(1)]
    public string Key { get; set; }

    [Column("Field", DbType.String, 3)]
    [PrimaryKey(2)]
    public string Field { get; set; }

    [Column("Value", DbType.String, 4)]
    public string Value { get; set; }

    [Column("ExpireAt", DbType.DateTime2, 5)]
    public DateTime? ExpireAt { get; set; }
}
