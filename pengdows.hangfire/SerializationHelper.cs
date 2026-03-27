namespace pengdows.hangfire;

public static class JsonHelper
{
    public static string Serialize(object value) => Hangfire.Common.SerializationHelper.Serialize(value, Hangfire.Common.SerializationOption.User);
    public static T Deserialize<T>(string value) => Hangfire.Common.SerializationHelper.Deserialize<T>(value, Hangfire.Common.SerializationOption.User)!;
}
