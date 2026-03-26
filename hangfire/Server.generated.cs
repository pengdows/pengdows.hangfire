using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "Server")]
public partial class Server
{
    [Column("Id", DbType.String, 1)]
    [Id]
    public string ID { get; set; }

    [Column("Data", DbType.String, 2)]
    public string Data { get; set; }

    [Column("LastHeartbeat", DbType.DateTime, 3)]
    public DateTime LastHeartbeat { get; set; }
}
