namespace Rinha;

public sealed class ClusterOptions
{
    public required string Ip { get; set; }
    public required int Port { get; set; }
    public required string[] Seeds { get; set; }
}