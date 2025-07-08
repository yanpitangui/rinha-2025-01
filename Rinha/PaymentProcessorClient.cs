using System.Threading.Channels;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Rinha;

public sealed class PaymentProcessorActor : ReceiveActor
{
    private readonly string _key;
    private readonly HttpClient _client;
    private readonly IActorRef _stats;

    public PaymentProcessorActor(string key, HttpClient httpClient, IActorRef stats)
    {
        _key = key;
        _client = httpClient;
        _stats = stats;
        var writer = StartStream();
        Receive<PaymentRequest>(request => writer.WriteAsync(request).PipeTo(Self));
    }

    private ChannelWriter<PaymentRequest> StartStream()
    {
       var (writer, source) = Source.Channel<PaymentRequest>(1000).PreMaterialize(Context.System);
       source
           .SelectAsyncUnordered(10, RequestPayment)
           .To(Sink.Ignore<PaymentRequest>())
           .Run(Context.Materializer());
       
       return writer;
    }

    private async Task<PaymentRequest> RequestPayment(PaymentRequest request)
    {
        var requestedAt = DateTime.UtcNow;
        var response = await _client.PostAsJsonAsync("/payments", new
        {
            request.Amount,
            request.CorrelationId,
            requestedAt,
        });

        if (response.IsSuccessStatusCode)
        {
            _stats.Tell(new PaymentStatsActor.Commands.PaymentProcessed(_key, request.Amount, requestedAt));
        }

        return request;
    }
}