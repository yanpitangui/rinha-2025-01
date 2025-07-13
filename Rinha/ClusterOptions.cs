namespace Rinha;

public sealed class ClusterOptions
{
    public string Ip { get; set; } = null!;
    public int Port { get; set; }
    public string[] Seeds { get; set; } = [];
}