using System;
using System.Data;
using System.Threading.Tasks;
using pengdows.hangfire.models;
using pengdows.crud;

namespace pengdows.hangfire.gateways;

public sealed class CounterGateway : TableGateway<Counter, long>, ICounterGateway
{
    public CounterGateway(IDatabaseContext context) : base(context) { }

    public async Task AppendAsync(string key, int delta, DateTime? expireAt = null, IDatabaseContext? context = null)
    {
        var ctx = context ?? Context;
        await CreateAsync(new Counter { Key = key, Value = delta, ExpireAt = expireAt }, ctx);
    }

    /// <summary>
    /// Groups Counter rows by Key, sums their Values, upserts into AggregatedCounter, then deletes the processed rows.
    /// Returns the number of Counter rows processed.
    /// </summary>
    public async Task<int> AggregateAsync(int batchSize)
    {
        // Read a batch of counter rows
        await using var readSc = Context.CreateSqlContainer();
        readSc.AppendQuery("SELECT ")
              .AppendName("Id").AppendComma()
              .AppendName("Key").AppendComma()
              .AppendName("Value")
              .AppendQuery(" FROM ").AppendQuery(WrappedTableName)
              .AppendQuery(" ORDER BY ").AppendName("Id").AppendQuery(" ASC");
        Context.Dialect.AppendPaging(readSc.Query, 0, batchSize);

        var rows = new List<(long Id, string Key, int Value)>();
        await using (var reader = await readSc.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2)));
            }
        }

        if (rows.Count == 0)
        {
            return 0;
        }

        // Group and upsert into AggregatedCounter
        var aggregatedTableName = new AggregatedCounterGateway(Context).WrappedTableName;
        foreach (var (aggKey, aggValue) in rows.GroupBy(r => r.Key).Select(g => (g.Key, g.Sum(r => (long)r.Value))))
        {
            await using var upsertSc = Context.CreateSqlContainer();
            if (Context.Product == pengdows.crud.enums.SupportedDatabase.SqlServer)
            {
                upsertSc.AppendQuery("MERGE ").AppendQuery(aggregatedTableName).AppendQuery(" WITH (HOLDLOCK) AS t")
                    .AppendQuery(" USING (VALUES (");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("key", DbType.String, aggKey)).AppendComma();
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("val", DbType.Int64, aggValue))
                    .AppendQuery(")) AS s (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(")")
                    .AppendQuery(" ON t.").AppendName("Key").AppendQuery(" = s.").AppendName("Key")
                    .AppendQuery(" WHEN MATCHED THEN UPDATE SET t.").AppendName("Value")
                    .AppendQuery(" = t.").AppendName("Value").AppendQuery(" + s.").AppendName("Value")
                    .AppendQuery(" WHEN NOT MATCHED THEN INSERT (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(")")
                    .AppendQuery(" VALUES (s.").AppendName("Key").AppendQuery(", s.").AppendName("Value").AppendQuery(");");
            }
            else if (Context.Product == pengdows.crud.enums.SupportedDatabase.Oracle)
            {
                // Oracle does not support ON CONFLICT; use MERGE INTO ... USING DUAL
                upsertSc.AppendQuery("MERGE INTO ").AppendQuery(aggregatedTableName).AppendQuery(" t")
                    .AppendQuery(" USING (SELECT ");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("key", DbType.String, aggKey));
                upsertSc.AppendQuery(" AS ").AppendName("Key").AppendComma();
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("val", DbType.Int64, aggValue));
                upsertSc.AppendQuery(" AS ").AppendName("Value").AppendQuery(" FROM DUAL) s")
                    .AppendQuery(" ON (t.").AppendName("Key").AppendQuery(" = s.").AppendName("Key").AppendQuery(")")
                    .AppendQuery(" WHEN MATCHED THEN UPDATE SET t.").AppendName("Value")
                    .AppendQuery(" = t.").AppendName("Value").AppendQuery(" + s.").AppendName("Value")
                    .AppendQuery(" WHEN NOT MATCHED THEN INSERT (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(")")
                    .AppendQuery(" VALUES (s.").AppendName("Key").AppendQuery(", s.").AppendName("Value").AppendQuery(")");
            }
            else if (Context.Product is pengdows.crud.enums.SupportedDatabase.MySql
                                        or pengdows.crud.enums.SupportedDatabase.MariaDb
                                        or pengdows.crud.enums.SupportedDatabase.TiDb)
            {
                // MySQL/MariaDB/TiDB do not support ON CONFLICT; use ON DUPLICATE KEY UPDATE.
                upsertSc.AppendQuery("INSERT INTO ").AppendQuery(aggregatedTableName)
                    .AppendQuery(" (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(") VALUES (");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("key", DbType.String, aggKey)).AppendComma();
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("val", DbType.Int64, aggValue))
                    .AppendQuery(") ON DUPLICATE KEY UPDATE ").AppendName("Value")
                    .AppendQuery(" = ").AppendName("Value").AppendQuery(" + VALUES(").AppendName("Value").AppendQuery(")");
            }
            else if (Context.Product == pengdows.crud.enums.SupportedDatabase.Firebird)
            {
                // Firebird does not support ON CONFLICT; uses MERGE with FROM RDB$DATABASE.
                // Explicit CASTs are required: Firebird cannot infer parameter types in a
                // USING (SELECT ... FROM RDB$DATABASE) clause without them.
                upsertSc.AppendQuery("MERGE INTO ").AppendQuery(aggregatedTableName).AppendQuery(" t")
                    .AppendQuery(" USING (SELECT CAST(");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("key", DbType.String, aggKey));
                upsertSc.AppendQuery(" AS VARCHAR(100)) AS ").AppendName("Key").AppendComma();
                upsertSc.AppendQuery(" CAST(");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("val", DbType.Int64, aggValue));
                upsertSc.AppendQuery(" AS BIGINT) AS ").AppendName("Value").AppendQuery(" FROM RDB$DATABASE) s")
                    .AppendQuery(" ON (t.").AppendName("Key").AppendQuery(" = s.").AppendName("Key").AppendQuery(")")
                    .AppendQuery(" WHEN MATCHED THEN UPDATE SET t.").AppendName("Value")
                    .AppendQuery(" = t.").AppendName("Value").AppendQuery(" + s.").AppendName("Value")
                    .AppendQuery(" WHEN NOT MATCHED THEN INSERT (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(")")
                    .AppendQuery(" VALUES (s.").AppendName("Key").AppendQuery(", s.").AppendName("Value").AppendQuery(")");
            }
            else
            {
                upsertSc.AppendQuery("INSERT INTO ").AppendQuery(aggregatedTableName)
                    .AppendQuery(" (").AppendName("Key").AppendComma().AppendName("Value").AppendQuery(") VALUES (");
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("key", DbType.String, aggKey)).AppendComma();
                upsertSc.AppendParam(upsertSc.AddParameterWithValue("val", DbType.Int64, aggValue))
                    .AppendQuery(") ON CONFLICT (").AppendName("Key").AppendQuery(")")
                    .AppendQuery(" DO UPDATE SET ").AppendName("Value")
                    .AppendQuery(" = ").AppendQuery(aggregatedTableName).AppendQuery(".").AppendName("Value")
                    .AppendQuery(" + EXCLUDED.").AppendName("Value");
            }
            await upsertSc.ExecuteNonQueryAsync();
        }

        // Delete the processed rows by Id
        var ids = rows.Select(r => r.Id).ToList();
        await using var delSc = Context.CreateSqlContainer();
        delSc.AppendQuery("DELETE FROM ").AppendQuery(WrappedTableName).AppendWhere();
        delSc.AppendName("Id").AppendIn();
        for (int i = 0; i < ids.Count; i++)
        {
            if (i > 0)
            {
                delSc.AppendComma();
            }
            delSc.AppendParam(delSc.AddParameterWithValue($"id{i}", DbType.Int64, ids[i]));
        }
        delSc.AppendCloseParen();
        await delSc.ExecuteNonQueryAsync();

        return rows.Count;
    }
}
