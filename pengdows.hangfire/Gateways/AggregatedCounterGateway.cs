using System.Data;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class AggregatedCounterGateway : TableGateway<AggregatedCounter, string>, IAggregatedCounterGateway
{
    public AggregatedCounterGateway(IDatabaseContext context) : base(context) { }

    public async Task<Dictionary<string, long>> GetTimelineAsync(string[] keys)
    {
        var result = new Dictionary<string, long>();
        if (keys.Length == 0)
        {
            return result;
        }

        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT ").AppendName("Key").AppendComma().AppendName("Value")
          .AppendQuery(" FROM ").AppendQuery(WrappedTableName)
          .AppendWhere().AppendName("Key").AppendIn();
        for (int i = 0; i < keys.Length; i++)
        {
            if (i > 0)
            {
                sc.AppendComma();
            }

            sc.AppendParam(sc.AddParameterWithValue($"k{i}", DbType.String, keys[i]));
        }
        sc.AppendCloseParen();

        await using var reader = await sc.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result[reader.GetString(0)] = reader.GetInt64(1);
        }

        return result;
    }

    public async Task<long> GetValueAsync(string key)
    {
        await using var sc = Context.CreateSqlContainer();
        sc.AppendQuery("SELECT ").AppendName("Value")
          .AppendQuery(" FROM ").AppendQuery(WrappedTableName).AppendWhere();
        sc.AppendName("Key").AppendEquals().AppendParam(sc.AddParameterWithValue("key", DbType.String, key));
        return await sc.ExecuteScalarOrNullAsync<long?>() ?? 0L;
    }

    public async Task<int> DeleteExpiredAsync(int batchSize)
    {
        var sc = BuildBaseRetrieve("a");
        sc.AppendWhere();
        sc.AppendName("a.ExpireAt").AppendQuery(" < ").AppendParam(sc.AddParameterWithValue("now", DbType.DateTime, DateTime.UtcNow));
        sc.AppendQuery(" ORDER BY ").AppendName("a.Key").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(sc.Query, 0, batchSize);
        var expired = await LoadListAsync(sc);
        if (expired.Count == 0)
        {
            return 0;
        }

        return await BatchDeleteAsync(expired.Select(e => e.Key));
    }
}
