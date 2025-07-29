using System.Text.Json.Serialization;
using Rinha.Actors;

namespace Rinha;


[JsonSerializable(typeof(PaymentProcessorActor.ProcessorPaymentRequest))]
public partial class WorkerContext : JsonSerializerContext
{
    
}