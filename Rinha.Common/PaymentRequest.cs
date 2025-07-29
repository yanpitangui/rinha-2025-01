namespace Rinha.Common;

public record PaymentRequest(Guid CorrelationId, decimal Amount);