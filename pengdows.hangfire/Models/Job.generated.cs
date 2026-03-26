using System;
using System.Data;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models;

[Table("Job", "HangFire")]
public partial class Job
{
    [Column("Id", DbType.Int64, 1)]
    [Id(writable: false)]
    public long ID { get; set; }

    [Column("StateId", DbType.Int64, 2)]
    public long? StateID { get; set; }

    [Column("StateName", DbType.String, 3)]
    public string StateName { get; set; }

    [Column("InvocationData", DbType.String, 4)]
    public string InvocationData { get; set; }

    [Column("Arguments", DbType.String, 5)]
    public string Arguments { get; set; }

    [Column("CreatedAt", DbType.DateTime, 6)]
    [CreatedOn]
    public DateTime CreatedAt { get; set; }

    [Column("ExpireAt", DbType.DateTime, 7)]
    public DateTime? ExpireAt { get; set; }
}
