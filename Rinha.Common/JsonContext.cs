using System.Text.Json.Serialization;

namespace Rinha.Common;

[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
public partial class JsonContext : JsonSerializerContext {}