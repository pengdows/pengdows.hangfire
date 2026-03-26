using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data;
using pengdows.crud;

namespace pengdows.hangfire.integration.tests;

public static class GatewayTestExtensions
{
    public static async Task<List<TEntity>> GetWhereAsync<TEntity, TRowID>(
        this ITableGateway<TEntity, TRowID> gateway, string column, object value)
        where TEntity : class, new()
    {
        var sc = gateway.BuildBaseRetrieve("");
        var dbType = value switch { long or int => DbType.Int64, _ => DbType.String };
        sc.AppendWhere().AppendName(column).AppendEquals().AppendParam(sc.AddParameterWithValue("val", dbType, value));
        return await gateway.LoadListAsync(sc).AsTask();
    }

    public static async Task<List<TEntity>> GetWhereAsync<TEntity>(
        this IPrimaryKeyTableGateway<TEntity> gateway, string column, object value)
        where TEntity : class, new()
    {
        var sc = gateway.BuildBaseRetrieve("");
        var dbType = value switch { long or int => DbType.Int64, _ => DbType.String };
        sc.AppendWhere().AppendName(column).AppendEquals().AppendParam(sc.AddParameterWithValue("val", dbType, value));
        return await gateway.LoadListAsync(sc).AsTask();
    }

    public static async Task<TEntity?> RetrieveOneAsync<TEntity, TRowID>(
        this ITableGateway<TEntity, TRowID> gateway, TRowID id)
        where TEntity : class, new()
    {
        return await gateway.RetrieveOneAsync(id).AsTask();
    }

    public static async Task CreateAsync<TEntity, TRowID>(
        this ITableGateway<TEntity, TRowID> gateway, TEntity entity)
        where TEntity : class, new()
    {
        await gateway.CreateAsync(entity);
    }
}
