using System.Text;
using Confluent.Kafka;

const string kafkaAddress = "host.docker.internal:9092";
const string targetFolder = "./source";
const string topicName = "mq.processingService";
const int defaultChunkSize = 900000 /* ~0.9 mb */;

Directory.CreateDirectory(targetFolder);
var fileSystemWatcher = new FileSystemWatcher(targetFolder);

var kafkaConfig = new ProducerConfig()
{
    BootstrapServers = kafkaAddress,
};

var producer = new ProducerBuilder<Null, byte[]>(kafkaConfig)
    .SetValueSerializer(Serializers.ByteArray).Build();

fileSystemWatcher.Created += async (_, eventArgs) => await CaptureFileAsync(eventArgs.FullPath, defaultChunkSize);

try
{
    var cancellationTask = SetupCancellation(out var token);
    
    await cancellationTask;
}
catch (OperationCanceledException )
{
    Console.WriteLine("Application was shutdown gracefully...");
}
finally
{
    producer.Dispose();
}


async Task CaptureFileAsync(string filePath, int chunkSize)
{
    var fileInfo = new FileInfo(filePath);
    var fileName = fileInfo.Name;
    var totalChunks = (int)Math.Ceiling((decimal)fileInfo.Length / chunkSize);
    int currentChunk = default;

    var partition = new TopicPartition(topicName, Partition.Any);

    await foreach (var chunk in ReadByChunksAsync(filePath, defaultChunkSize))
    {
        var headers = new Headers()
        {
            {"FileName", Encoding.Default.GetBytes(fileName)},
            {"Position", Encoding.Default.GetBytes($"{++currentChunk}")},
            {"TotalChunks", Encoding.Default.GetBytes(totalChunks.ToString())},
        };

        await SendToProcessingServiceAsync(producer, chunk, partition, headers);
    }
}

static async IAsyncEnumerable<byte[]> ReadByChunksAsync(string filePath, int chunkSize)
{
    await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
    var buffer = new byte[chunkSize];

    while (await fs.ReadAsync(buffer) != 0)
    {
        yield return buffer;
    }
}

static async Task SendToProcessingServiceAsync<TKey, TValue>(IProducer<TKey, TValue> producer, TValue data, TopicPartition partition, Headers? headers)
{
    var message = new Message<TKey, TValue>()
    {
        Headers = headers,
        Value = data,
    };

    await producer.ProduceAsync(partition, message);
}

static Task SetupCancellation(out CancellationToken token)
{
    var cts = new CancellationTokenSource();

    var cancellationTask = Task.Factory.StartNew(() =>
    {
        while (cts.Token.IsCancellationRequested)
        {
            if (Console.ReadKey(true).Key == ConsoleKey.Escape) cts.Cancel();
        }
    }, TaskCreationOptions.LongRunning);

    token = cts.Token;

    return cancellationTask;
}
