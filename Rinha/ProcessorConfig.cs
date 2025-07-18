namespace Rinha;

public record ProcessorConfig
{
    public int RequestPaymentParallelism { get; init; }
    public int PersistPaymentsParallelism { get; init; }
    public int GroupSize { get; init; }
    public int Timeout { get; init; }
}