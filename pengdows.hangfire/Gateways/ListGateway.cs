using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class ListGateway : TableGateway<List, long>, IListGateway
{
    public ListGateway(IDatabaseContext context) : base(context) { }

    public async Task AppendAsync(string key, string value, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await CreateAsync(new List { Key = key, Value = value }, ctx);
    }

    public async Task DeleteByKeyValueAsync(string key, string value, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendAnd();
        if (ctx.Product == pengdows.crud.enums.SupportedDatabase.Oracle)
        {
            // Oracle cannot compare CLOB columns with = in SQL; use DBMS_LOB.COMPARE instead.
            sc.AppendQuery("DBMS_LOB.COMPARE(").AppendName("Value")
              .AppendQuery(", TO_CLOB(").AppendParam(sc.AddParameterWithValue("value", DbType.String, value))
              .AppendQuery(")) = 0");
        }
        else
        {
            sc.AppendName("Value").AppendEquals().AppendParam(sc.AddParameterWithValue("value", DbType.String, value));
        }
        await sc.ExecuteNonQueryAsync();
    }

    public async Task TrimAsync(string key, int keepStartingFrom, int keepEndingAt, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await using var sc = ctx.CreateSqlContainer();
        sc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));

        var isMySqlFamily = ctx.Product is pengdows.crud.enums.SupportedDatabase.MySql
                                        or pengdows.crud.enums.SupportedDatabase.MariaDb
                                        or pengdows.crud.enums.SupportedDatabase.TiDb;

        if (isMySqlFamily)
        {
            // MySQL rejects LIMIT inside a NOT IN subquery unless the subquery is
            // wrapped in a derived table.  Wrap it:
            //   NOT IN (SELECT Id FROM (SELECT Id ... LIMIT N) AS _t)
            sc.AppendQuery(" AND ").AppendName("Id").AppendQuery(" NOT IN (SELECT ").AppendName("Id")
              .AppendQuery(" FROM (SELECT ").AppendName("Id")
              .AppendQuery(" FROM ").AppendQuery(WrappedTableName).AppendWhere();
            sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key2", DbType.String, key));
            sc.AppendQuery(" ORDER BY ").AppendName("Id");
            ctx.Dialect.AppendPaging(sc.Query, keepStartingFrom, keepEndingAt - keepStartingFrom + 1);
            sc.AppendQuery(") AS _t)");
        }
        else
        {
            sc.AppendQuery(" AND ").AppendName("Id").AppendQuery(" NOT IN (SELECT ").AppendName("Id")
              .AppendQuery(" FROM ").AppendQuery(WrappedTableName).AppendWhere();
            sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key2", DbType.String, key));
            sc.AppendQuery(" ORDER BY ").AppendName("Id");
            ctx.Dialect.AppendPaging(sc.Query, keepStartingFrom, keepEndingAt - keepStartingFrom + 1);
            sc.AppendCloseParen();
        }

        await sc.ExecuteNonQueryAsync();
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

    public async Task<List<string>> GetRangeAsync(string key, int from, int to)
    {
        var sc = BuildBaseRetrieve("l");
        sc.AppendWhere();
        sc.AppendName("l.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendQuery(" ORDER BY ").AppendName("l.Id").AppendQuery(" DESC");
        Context.Dialect.AppendPaging(sc.Query, from, to - from + 1);
        var items = await LoadListAsync(sc);
        return items.Select(l => l.Value ?? string.Empty).ToList();
    }

    public async Task<List<string>> GetAllAsync(string key)
    {
        var sc = BuildBaseRetrieve("l");
        sc.AppendWhere();
        sc.AppendName("l.Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        sc.AppendQuery(" ORDER BY ").AppendName("l.Id").AppendQuery(" DESC");
        var items = await LoadListAsync(sc);
        return items.Select(l => l.Value ?? string.Empty).ToList();
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
        var sc = BuildBaseRetrieve("l");
        sc.AppendWhere();
        sc.AppendName("l.ExpireAt").AppendQuery(" < ").AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendQuery(" ORDER BY ").AppendName("l.Key").AppendQuery(", ").AppendName("l.Id").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, 0, batchSize);
        var expired = await LoadListAsync(sc);
        if (expired.Count == 0)
        {
            return 0;
        }

        return await BatchDeleteAsync(expired.Select(l => l.ID));
    }
}
