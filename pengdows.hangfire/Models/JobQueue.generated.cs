using System;
using System.Data;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models;

[Table("JobQueue", "HangFire")]
public partial class JobQueue
{
    [Column("Id", DbType.Int64, 1)]
    [Id(writable: false)]
    public long ID { get; set; }

    [Column("JobId", DbType.Int64, 2)]
    public long JobID { get; set; }

    [Column("Queue", DbType.String, 3)]
    [PrimaryKey(1)]
    public string Queue { get; set; }

    [Column("FetchedAt", DbType.DateTime, 4)]
    public DateTime? FetchedAt { get; set; }
}
