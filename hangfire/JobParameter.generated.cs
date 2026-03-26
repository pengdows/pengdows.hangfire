using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "JobParameter")]
public partial class JobParameter
{
    [Column("JobId", DbType.Int64, 2)]
    [PrimaryKey(1)]
    public long JobID { get; set; }

    [Column("Name", DbType.String, 3)]
    [PrimaryKey(2)]
    public string Name { get; set; }

    [Column("Value", DbType.String, 4)]
    public string Value { get; set; }
}
