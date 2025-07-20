namespace Rinha;

public record PersisterConfig
{
    public int PersistPaymentsParallelism { get; init; }
    public int GroupSize { get; init; }
    public int Timeout { get; init; }
}