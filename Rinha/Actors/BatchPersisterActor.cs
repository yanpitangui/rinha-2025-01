using System.Threading.Channels;
using Akka.Streams;
using Akka.Streams.Dsl;
using Npgsql;

namespace Rinha.Actors;

public class BatchPersister
{
    private readonly NpgsqlDataSource _source;
    
    public BatchPersister(NpgsqlDataSource source)
    {
        _source = source;
    }
    
    public ChannelWriter<Commands.PersistPayment> StartStream(PersisterConfig config, IMaterializer materializer)
    {
        var (mainWriter, mainSource) = Source.Channel<Commands.PersistPayment>
                (10000, fullMode: BoundedChannelFullMode.Wait)
            .PreMaterialize(materializer);
        mainSource
            .AddAttributes(Attributes.CreateInputBuffer(1, 1))
            .GroupedWithin(config.GroupSize, TimeSpan.FromMilliseconds(config.Timeout))
            .SelectAsync(config.PersistPaymentsParallelism, PersistPayments)
            .To(Sink.Ignore<List<Commands.PersistPayment>>())
            .Run(materializer);

        return mainWriter;
    }
    
    
    private async Task<List<Commands.PersistPayment>> PersistPayments(IEnumerable<Commands.PersistPayment> batch)
    {
        await using var conn = await _source.OpenConnectionAsync();

        var batchList = batch.ToList();

        await using var writer = await conn.BeginBinaryImportAsync(
            "COPY payments (correlation_id, processor, amount, requested_at) FROM STDIN (FORMAT BINARY)");
        foreach (var payment in batchList)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(payment.CorrelationId);
            await writer.WriteAsync(payment.Key);
            await writer.WriteAsync(payment.Amount);
            await writer.WriteAsync(payment.RequestedAt);
        }
        await writer.CompleteAsync();

        foreach (var payment in batchList)
        {
            payment.Tcs.SetResult();
        }
        return batchList;
        
    }

    public static class Commands
    {
        public record PersistPayment(Guid CorrelationId, decimal Amount, DateTimeOffset RequestedAt, string Key, TaskCompletionSource Tcs);
    }
}