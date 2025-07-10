namespace Rinha;

public record PaymentRequest(Guid CorrelationId, decimal Amount, int RetryCount = 0);