using System.Text.Json.Serialization;

namespace Rinha.Common;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(PaymentRequest))]
[JsonSerializable(typeof(PaymentSummaryResponse))]
public partial class JsonContext : JsonSerializerContext {}