namespace pengdows.hangfire;

using System.Collections.Generic;
using Hangfire;
using Hangfire.Server;
using Hangfire.Storage;
using pengdows.hangfire.gateways;
using pengdows.crud;

public sealed class PengdowsCrudJobStorage : JobStorage
{
    private static readonly HashSet<string> SupportedFeatures = new()
    {
        JobStorageFeatures.ExtendedApi,
        JobStorageFeatures.JobQueueProperty,
        JobStorageFeatures.ProcessesInsteadOfComponents,
        JobStorageFeatures.Connection.GetUtcDateTime,
        JobStorageFeatures.Connection.GetSetContains,
        JobStorageFeatures.Connection.LimitedGetSetCount,
        JobStorageFeatures.Connection.BatchedGetFirstByLowest,
        JobStorageFeatures.Transaction.AcquireDistributedLock,
        JobStorageFeatures.Monitoring.DeletedStateGraphs,
        JobStorageFeatures.Monitoring.AwaitingJobs,
    };

    internal PengdowsCrudStorageOptions Options { get; }
    internal IDatabaseContext DatabaseContext { get; }
    internal StorageClock Clock { get; }
    internal IJobGateway Jobs { get; }
    internal IJobQueueGateway JobQueues { get; }
    internal IJobStateGateway JobStates { get; }
    internal IJobParameterGateway JobParameters { get; }
    internal IServerGateway Servers { get; }
    internal IDistributedLockGateway Locks { get; }
    internal IHashGateway Hashes { get; }
    internal ISetGateway Sets { get; }
    internal IListGateway Lists { get; }
    internal ICounterGateway Counters { get; }
    internal IAggregatedCounterGateway AggregatedCounters { get; }

    public PengdowsCrudJobStorage(IDatabaseContext databaseContext, PengdowsCrudStorageOptions? options = null)
    {
        DatabaseContext = databaseContext ?? throw new ArgumentNullException(nameof(databaseContext));
        Options = options ?? new PengdowsCrudStorageOptions();
        Clock = new StorageClock(() => new PengdowsCrudConnection(this).ReadDatabaseUtcNow());

        Jobs = new JobGateway(DatabaseContext, () => Clock.UtcNow);
        JobQueues = new JobQueueGateway(DatabaseContext, () => Clock.UtcNow);
        JobStates = new JobStateGateway(DatabaseContext);
        JobParameters = new JobParameterGateway(DatabaseContext);
        Servers = new ServerGateway(DatabaseContext, () => Clock.UtcNow);
        Locks = new DistributedLockGateway(DatabaseContext);
        Hashes = new HashGateway(DatabaseContext, () => Clock.UtcNow);
        Sets = new SetGateway(DatabaseContext, () => Clock.UtcNow);
        Lists = new ListGateway(DatabaseContext, () => Clock.UtcNow);
        Counters = new CounterGateway(DatabaseContext);
        AggregatedCounters = new AggregatedCounterGateway(DatabaseContext, () => Clock.UtcNow);
    }

    public override IMonitoringApi GetMonitoringApi() => new PengdowsCrudMonitoringApi(this);
    public override IStorageConnection GetConnection() => new PengdowsCrudConnection(this);

    public override IEnumerable<IBackgroundProcess> GetStorageWideProcesses()
    {
        yield return new ExpirationManager(this, Options.JobExpirationCheckInterval);
        yield return new CountersAggregator(this, Options.CountersAggregateInterval);
        yield return new FetchedJobWatchdog(this, Options.InvisibilityTimeout);
    }

    public override bool HasFeature(string featureId) =>
        SupportedFeatures.Contains(featureId) || base.HasFeature(featureId);

    public void Initialize()
    {
        var shouldInstall = Options.AutoPrepareSchema == true;
        if (Options.AutoPrepareSchema is null && DatabaseContext.Product == pengdows.crud.enums.SupportedDatabase.SqlServer)
        {
            // The default is intentionally non-destructive for fresh databases,
            // but existing SQL Server schemas still need embedded migrations.
            using var sc = DatabaseContext.CreateSqlContainer(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = 'HangFire' AND TABLE_NAME = 'Schema') THEN 1 ELSE 0 END");
            shouldInstall = sc.ExecuteScalarOrNullAsync<int?>().GetAwaiter().GetResult() == 1;
        }

        if (shouldInstall)
        {
            var installer = new PengdowsCrudSchemaInstaller(DatabaseContext);
            installer.InstallAsync().GetAwaiter().GetResult();
        }

        try
        {
            // Resolve the complete JobQueue projection so a missing FetchToken
            // column is reported during startup rather than on the first claim.
            JobQueues.GetPagedByQueueAsync("__pengdows_schema_probe__", 0, 1, false)
                .GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "pengdows.hangfire 2.0.6 requires the JobQueue.FetchToken column; apply schema v11 / Liquibase changeset 13.", ex);
        }
    }
}
