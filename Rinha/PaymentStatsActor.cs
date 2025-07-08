using Akka.Actor;

namespace Rinha;

public sealed class PaymentStatsActor : ReceiveActor
{
    private readonly List<Commands.PaymentProcessed> _defaultPayments = new();
    private readonly List<Commands.PaymentProcessed> _fallbackPayments = new();

    public PaymentStatsActor()
    {
        Receive<Commands.PaymentProcessed>(HandlePaymentProcessed);
        Receive<Commands.GetPaymentSummary>(HandleGetSummary);
    }

    private void HandlePaymentProcessed(Commands.PaymentProcessed msg)
    {
        var list = msg.Processor switch
        {
            "default" => _defaultPayments,
            "fallback" => _fallbackPayments,
            _ => null
        };

        if (list is null) return;

        list.Add(msg);
    }

    private void HandleGetSummary(Commands.GetPaymentSummary req)
    {
        var from = req.From ?? DateTimeOffset.MinValue;
        var to = req.To ?? DateTimeOffset.MaxValue;

        var def = _defaultPayments
            .Where(p => p.Timestamp >= from && p.Timestamp <= to)
            .ToList();

        var fb = _fallbackPayments
            .Where(p => p.Timestamp >= from && p.Timestamp <= to)
            .ToList();

        Sender.Tell(new PaymentSummaryResponse(
            new PaymentSummaryItem(def.Count, def.Sum(p => p.Amount)),
            new PaymentSummaryItem(fb.Count, fb.Sum(p => p.Amount))
        ));
    }

    public static class Commands
    {
        public record PaymentProcessed(string Processor, decimal Amount, DateTimeOffset Timestamp);
        public record GetPaymentSummary(DateTimeOffset? From, DateTimeOffset? To);
    }
}

public record PaymentSummaryResponse(PaymentSummaryItem Default, PaymentSummaryItem Fallback);
public record PaymentSummaryItem(int TotalRequests, decimal TotalAmount);