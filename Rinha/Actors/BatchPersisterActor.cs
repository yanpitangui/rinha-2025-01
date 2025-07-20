using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Npgsql;

namespace Rinha.Actors;

public class BatchPersisterActor : ReceiveActor
{
    private readonly string _connectionString;
    
    public BatchPersisterActor(string connectionString, PersisterConfig config)
    {
        _connectionString = connectionString;
        var writer = StartStream(config);

        Receive<Commands.PersistPayment>(req => writer.Tell(req));
    }
    
    private IActorRef StartStream(PersisterConfig config)
    {
        var materializer = Context.Materializer();
        var (mainWriter, mainSource) = Source
            .ActorRef<Commands.PersistPayment>(5000, OverflowStrategy.DropTail)
            .PreMaterialize(materializer);
        
        mainSource
            .GroupedWithin(config.GroupSize, TimeSpan.FromMilliseconds(config.Timeout))
            .SelectAsync(config.PersistPaymentsParallelism, PersistPayments)
            .To(Sink.Ignore<IEnumerable<Commands.PersistPayment>>())
            .Run(materializer);

        return mainWriter;
    }
    
    
    private async Task<IEnumerable<Commands.PersistPayment>> PersistPayments(IEnumerable<Commands.PersistPayment> batch)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY payments (correlation_id, processor, amount, requested_at) FROM STDIN (FORMAT BINARY)");
        foreach (var payment in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(payment.CorrelationId);
            await writer.WriteAsync(payment.Key);
            await writer.WriteAsync(payment.Amount);
            await writer.WriteAsync(payment.RequestedAt);
        }
        await writer.CompleteAsync();
        
        return batch;
        
    }

    public static class Commands
    {
        public record PersistPayment(Guid CorrelationId, decimal Amount, DateTimeOffset RequestedAt, string Key);
    }
}