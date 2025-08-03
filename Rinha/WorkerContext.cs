using System.Text.Json.Serialization;
using Rinha.Actors;

namespace Rinha;


[JsonSerializable(typeof(PaymentPipelineActor.ProcessorPaymentRequest))]
public partial class WorkerContext : JsonSerializerContext
{
    
}