namespace Rinha;

public sealed record PaymentSummaryResponse(PaymentSummaryItem Default, PaymentSummaryItem Fallback);

public sealed record PaymentSummaryItem(int TotalRequests, decimal TotalAmount);