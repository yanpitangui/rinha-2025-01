namespace Rinha;

public record PipelineConfig
{
    public int HandlePaymentParallelism { get; init; }
    public int PersistPaymentsParallelism { get; init; }
    public int GroupSize { get; init; }
    public int Timeout { get; init; }
}