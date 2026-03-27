using System;
using Hangfire;
using pengdows.crud;
using pengdows.crud.enums;
using pengdows.crud.fakeDb;
using Xunit;

namespace pengdows.hangfire.tests;

public sealed class ConfigurationTests
{
    private class DummyConfig : IGlobalConfiguration
    {
        public IGlobalConfiguration UseStorage<T>(T storage) where T : JobStorage => this;
    }

    [Fact]
    public void UsePengdowsCrudStorage_ReturnsConfiguration()
    {
        var config = new DummyConfig();
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        
        var result = config.UsePengdowsCrudStorage(ctx);

        Assert.NotNull(result);
    }

    [Fact]
    public void UsePengdowsCrudStorage_NullConfig_Throws()
    {
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx = new DatabaseContext("Data Source=fake", factory);
        Assert.Throws<ArgumentNullException>(() => ((IGlobalConfiguration)null!).UsePengdowsCrudStorage(ctx));
    }

    [Fact]
    public void UsePengdowsCrudStorage_NullDb_Throws()
    {
        var config = new DummyConfig();
        Assert.Throws<ArgumentNullException>(() => config.UsePengdowsCrudStorage(null!));
    }

    [Fact]
    public void UsePengdowsCrudStorage_WithConfigureOptions_InvokesCallback()
    {
        var config  = new DummyConfig();
        var factory = new fakeDbFactory(SupportedDatabase.SqlServer);
        var ctx     = new DatabaseContext("Data Source=fake", factory);
        var called  = false;
        var result  = config.UsePengdowsCrudStorage(ctx, opts => { called = true; });
        Assert.True(called);
        Assert.NotNull(result);
    }
}
