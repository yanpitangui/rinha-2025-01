namespace Rinha;

public record PaymentRequest(Guid CorrelationId, decimal Amount);