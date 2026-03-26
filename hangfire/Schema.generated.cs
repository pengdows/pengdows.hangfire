using System;
using System.Data;
using System.Text.Json;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models.hangfire;

[Table("HangFire", "Schema")]
public partial class Schema
{
    [Column("Version", DbType.Int32, 1)]
    [Id]
    public int Version { get; set; }
}
