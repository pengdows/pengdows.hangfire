using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "State")]
public partial class State
{
    [Column("Id", DbType.Int64, 1)]
    [Id(writable: false)]
    public long ID { get; set; }

    [Column("JobId", DbType.Int64, 2)]
    [PrimaryKey(1)]
    public long JobID { get; set; }

    [Column("Name", DbType.String, 3)]
    public string Name { get; set; }

    [Column("Reason", DbType.String, 4)]
    public string Reason { get; set; }

    [Column("CreatedAt", DbType.DateTime, 5)]
    [CreatedOn]
    public DateTime CreatedAt { get; set; }

    [Column("Data", DbType.String, 6)]
    public string Data { get; set; }
}
