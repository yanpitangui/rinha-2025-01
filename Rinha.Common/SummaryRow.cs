namespace Rinha.Common;

public sealed record SummaryRow
{
    public SummaryRow() {}
    public SummaryRow(string Processor, int TotalRequests, decimal TotalAmount)
    {
        this.Processor = Processor;
        this.TotalRequests = TotalRequests;
        this.TotalAmount = TotalAmount;
    }

    public string Processor { get; init; }
    public int TotalRequests { get; init; }
    public decimal TotalAmount { get; init; }
}