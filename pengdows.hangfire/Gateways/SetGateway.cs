using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class SetGateway : PrimaryKeyTableGateway<Set>, ISetGateway
{
    public SetGateway(IDatabaseContext context) : base(context) { }

    public async Task<HashSet<string>> GetAllItemsAsync(string key)
    {
        var sc = BuildBaseRetrieve("s");
        sc.AppendWhere();
        sc.AppendName("s.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        var sets = await LoadListAsync(sc);
        return sets.Select(s => s.Value).ToHashSet()!;
    }

    public async Task<string?> GetFirstByLowestScoreAsync(string key, double fromScore, double toScore)
    {
        var result = await GetFirstByLowestScoreAsync(key, fromScore, toScore, 1);
        return result.FirstOrDefault();
    }

    public async Task<List<string>> GetFirstByLowestScoreAsync(string key, double fromScore, double toScore, int count)
    {
        var sc = BuildBaseRetrieve("s");
        sc.AppendWhere();
        sc.AppendName("s.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendAnd();
        sc.AppendName("s.Score").AppendQuery(" >= ").AppendParam(sc.AddParameterWithValue("fromScore", DbType.Double, fromScore));
        sc.AppendAnd();
        sc.AppendName("s.Score").AppendQuery(" <= ").AppendParam(sc.AddParameterWithValue("toScore", DbType.Double, toScore));
        sc.AppendQuery(" ORDER BY ").AppendName("s.Score").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, 0, count);
        var sets = await LoadListAsync(sc);
        return sets.Select(s => s.Value).ToList()!;
    }

    public async Task<long> GetCountAsync(string key)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT COUNT(*) FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        return await sc.ExecuteScalarRequiredAsync<long>();
    }

    public async Task<bool> ContainsAsync(string key, string value)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT COUNT(*) FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendAnd();
        sc.AppendName("Value").AppendEquals().AppendParam(sc.AddParameterWithValue("value", DbType.String, value));
        return await sc.ExecuteScalarRequiredAsync<long>() > 0;
    }

    public async Task<List<string>> GetRangeAsync(string key, int from, int to)
    {
        var sc = BuildBaseRetrieve("s");
        sc.AppendWhere();
        sc.AppendName("s.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendQuery(" ORDER BY ").AppendName("s.Score").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, from, to - from + 1);
        var sets = await LoadListAsync(sc);
        return sets.Select(s => s.Value).ToList()!;
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

    public async Task<int> DeleteByKeyAsync(string key, IDatabaseContext? context = null)
    {
        await using var sc = (context ?? Context).CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        return await sc.ExecuteNonQueryAsync();
    }

    public async Task<int> DeleteExpiredAsync(int batchSize)
    {
        var sc = BuildBaseRetrieve("s");
        sc.AppendWhere();
        sc.AppendName("s.ExpireAt").AppendQuery(" < ").AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendQuery(" ORDER BY ").AppendName("s.Key").AppendQuery(", ").AppendName("s.Value").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, 0, batchSize);
        var expired = await LoadListAsync(sc);
        if (expired.Count == 0)
        {
            return 0;
        }

        return await BatchDeleteAsync(expired);
    }
}
