// pengdows.hangfire.stress.tests.worker
//
// Minimal process that acquires a distributed lock and holds it until killed.
// Used by out-of-process crash tests in pengdows.hangfire.stress.tests.
//
// Usage: Worker <connectionString> <resource> <ttlSeconds>
//
// Output:
//   ACQUIRED        written to stdout once the lock is held (flushed immediately)
//   ERROR: <msg>    written to stderr if acquire fails
//
// Exit codes:
//   0  — process was killed while holding the lock (normal for crash tests)
//   1  — bad arguments
//   2  — acquire failed

using System;
using System.Threading;
using pengdows.hangfire;
using Microsoft.Data.SqlClient;
using pengdows.crud;
using pengdows.crud.configuration;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: Worker <connectionString> <resource> <ttlSeconds>");
    return 1;
}

var connectionString = args[0];
var resource         = args[1];
var ttlSeconds       = int.Parse(args[2]);

var ctx = new DatabaseContext(
    new DatabaseContextConfiguration { ConnectionString = connectionString },
    SqlClientFactory.Instance);

var storage = new PengdowsCrudJobStorage(ctx, new PengdowsCrudStorageOptions
{
    AutoPrepareSchema  = false,
    DistributedLockTtl = TimeSpan.FromSeconds(ttlSeconds),
});

try
{
    using var lk = new PengdowsCrudDistributedLock(
        storage, resource, TimeSpan.FromSeconds(30));

    // Signal parent: lock is held
    Console.WriteLine("ACQUIRED");
    Console.Out.Flush();

    // Hold until the process is killed
    Thread.Sleep(Timeout.Infinite);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"ERROR: {ex.GetType().Name}: {ex.Message}");
    return 2;
}

return 0;
