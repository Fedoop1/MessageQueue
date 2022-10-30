using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;

const string appName = "MessageQueue.CaptureService";
const string kafkaAddress = "host.docker.internal:9092";
const string targetFolder = "Source";
const string topicName = "mq.processingService";
const int defaultChunkSize = 900000 /* ~0.9 mb */;

Directory.CreateDirectory(targetFolder);

var fileSystemWatcher = new FileSystemWatcher(targetFolder)
{
    EnableRaisingEvents = true,
    Filter = "*.*",
    NotifyFilter = NotifyFilters.FileName,
};

try
{
    Console.WriteLine($"{appName} is running");

    var cancellationTask = SetupCancellation(out var token);

    fileSystemWatcher.Created += async (_, eventArgs) =>
    {
        Console.WriteLine("New file in source folder was detected..." +
                          $"File path: {eventArgs.FullPath}");

        var clientUid = $"{appName}.{Guid.NewGuid():N}";

        var kafkaConfig = new ProducerConfig()
        {
            BootstrapServers = kafkaAddress,
            ClientId = clientUid,
            //TransactionalId = appUid,
        };

        var producer = new ProducerBuilder<Null, byte[]>(kafkaConfig)
            .SetValueSerializer(Serializers.ByteArray).Build();

        try
        {
            //producer.BeginTransaction();
            await CaptureFileAsync(eventArgs.FullPath, defaultChunkSize, producer, token);
            //producer.CommitTransaction();
        }
        catch (Exception)
        {
            //producer.AbortTransaction();
            throw;
        }
        finally
        {
            producer.Dispose();
        }

        Console.WriteLine("File was successfully delivered..." +
                          $"File path: {eventArgs.FullPath}");
    };

    await cancellationTask;
}
catch (OperationCanceledException)
{
    Console.WriteLine("Application was shutdown gracefully...");
}

async Task CaptureFileAsync(string filePath, int chunkSize, IProducer<Null, byte[]> producer, CancellationToken token)
{
    token.ThrowIfCancellationRequested();

    var fileInfo = new FileInfo(filePath);
    var fileName = fileInfo.Name;
    var totalChunks = (int)Math.Ceiling((decimal)fileInfo.Length / chunkSize);
    int currentChunk = default;

    var partition = new TopicPartition(topicName, Partition.Any);

    await foreach (var chunk in ReadByChunksAsync(filePath, defaultChunkSize, token))
    {
        token.ThrowIfCancellationRequested();

        var headers = new Headers()
        {
            {"FileName", Encoding.Default.GetBytes(fileName)},
            {"Position", Encoding.Default.GetBytes($"{++currentChunk}")},
            {"TotalChunks", Encoding.Default.GetBytes(totalChunks.ToString())},
        };

        Console.WriteLine($"Sending {fileName}'s #{currentChunk} chunk...");
        await SendToProcessingServiceAsync(producer, chunk, partition, headers, token);
    }
}

static async IAsyncEnumerable<byte[]> ReadByChunksAsync(string filePath, int chunkSize, [EnumeratorCancellation] CancellationToken token)
{
    token.ThrowIfCancellationRequested();

    await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
    var buffer = new byte[chunkSize];

    while (await fs.ReadAsync(buffer) != 0)
    {
        token.ThrowIfCancellationRequested();
        yield return buffer;
    }
}

static async Task SendToProcessingServiceAsync(IProducer<Null, byte[]> producer, byte[] data, TopicPartition partition, Headers? headers, CancellationToken token)
{
    token.ThrowIfCancellationRequested();

    var message = new Message<Null, byte[]>
    {
        Headers = headers,
        Value = data,
    };

    await producer.ProduceAsync(partition, message, token);
}

static Task SetupCancellation(out CancellationToken token)
{
    var cts = new CancellationTokenSource();

    token = cts.Token;

    return Task.Factory.StartNew(() =>
    {
        Console.WriteLine("Press ESC to shutdown...");
        while (!cts.Token.IsCancellationRequested)
        {
            if (Console.ReadKey(true).Key == ConsoleKey.Escape) cts.Cancel();
        }
    }, TaskCreationOptions.LongRunning);
}
