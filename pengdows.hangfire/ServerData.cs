namespace pengdows.hangfire;

internal sealed class ServerData
{
    public string[] Queues      { get; set; } = Array.Empty<string>();
    public int      WorkerCount { get; set; }
    public DateTime StartedAt   { get; set; }
}
