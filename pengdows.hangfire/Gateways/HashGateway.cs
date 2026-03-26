using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class HashGateway : PrimaryKeyTableGateway<Hash>, IHashGateway
{
    public HashGateway(IDatabaseContext context) : base(context) { }

    public async Task<Dictionary<string, string>> GetAllEntriesAsync(string key)
    {
        var sc = BuildBaseRetrieve("h");
        sc.AppendWhere();
        sc.AppendName("h.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        var hashes = await LoadListAsync(sc);
        return hashes.ToDictionary(h => h.Field, h => h.Value ?? string.Empty);
    }

    public async Task<string?> GetValueAsync(string key, string field)
    {
        var sc = BuildBaseRetrieve("h");
        sc.AppendWhere();
        sc.AppendName("h.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendAnd();
        sc.AppendName("h.Field").AppendEquals().AppendParam(sc.AddParameterWithValue("field", DbType.String, field));
        var hash = await LoadSingleAsync(sc);
        return hash?.Value;
    }

    public async Task<long> GetCountAsync(string key)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT COUNT(*) FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        return await sc.ExecuteScalarRequiredAsync<long>();
    }

    public async Task<TimeSpan> GetTtlAsync(string key)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT MIN(").AppendName("ExpireAt").AppendQuery(") FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        var result = await sc.ExecuteScalarOrNullAsync<DateTime?>();
        if (result == null)
        {
            return TimeSpan.FromSeconds(-1);
        }
        var expiry = result.Value.Kind == DateTimeKind.Local
            ? result.Value.ToUniversalTime()
            : DateTime.SpecifyKind(result.Value, DateTimeKind.Utc);
        return expiry - DateTime.UtcNow;
    }

    public async Task DeleteAllForKeyAsync(string key, IDatabaseContext? context = null)
    {
        await using var sc = (context ?? Context).CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> UpdateExpireAtAsync(string key, DateTime? expireAt, IDatabaseContext? context = null)
    {
        await using var sc = (context ?? Context).CreateSqlContainer();
        sc.AppendQuery("UPDATE ").AppendQuery(WrappedTableName).AppendQuery(" SET ");
        sc.AppendName("ExpireAt").AppendEquals()
          .AppendParam(sc.AddParameterWithValue("expireAt", DbType.DateTime, expireAt as object ?? DBNull.Value));
        sc.AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> DeleteExpiredAsync(int batchSize)
    {
        var sc = BuildBaseRetrieve("h");
        sc.AppendWhere();
        sc.AppendName("h.ExpireAt").AppendQuery(" < ").AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendQuery(" ORDER BY ").AppendName("h.Key").AppendQuery(", ").AppendName("h.Field").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, 0, batchSize);
        var expired = await LoadListAsync(sc);
        if (expired.Count == 0)
        {
            return 0;
        }

        return await BatchDeleteAsync(expired);
    }
}
