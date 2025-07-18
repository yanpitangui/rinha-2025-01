namespace Rinha;

public record PoolConfig
{
    public int DefaultPoolSize { get; init; }
    public int FallbackPoolSize { get; init; }
    public int RouterPoolSize { get; init; }
}