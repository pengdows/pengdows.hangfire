namespace pengdows.hangfire;

using System;
using Hangfire;
using pengdows.crud;

public static class PengdowsCrudGlobalConfigurationExtensions
{
    public static IGlobalConfiguration<PengdowsCrudJobStorage> UsePengdowsCrudStorage(
        this IGlobalConfiguration configuration,
        IDatabaseContext db,
        Action<PengdowsCrudStorageOptions>? configureOptions = null)
    {
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (db == null)
        {
            throw new ArgumentNullException(nameof(db));
        }

        var options = new PengdowsCrudStorageOptions();
        configureOptions?.Invoke(options);

        var storage = new PengdowsCrudJobStorage(db, options);
        storage.Initialize();

        return configuration.UseStorage(storage);
    }
}
