using System.Data;
using pengdows.crud;
using pengdows.crud.attributes;

namespace pengdows.hangfire.models;

[Table("Schema", "HangFire")]
public partial class Schema
{
    [Column("Version", DbType.Int32, 1)]
    [Id]
    public int Version { get; set; }
}
