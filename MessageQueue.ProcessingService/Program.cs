using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using MessageQueue.ProcessingService;

const string appName = "MessageQueue.ProcessingService";
const string kafkaAddress = "host.docker.internal:9092";
const string targetFolder = "Destination";
const string topicName = "mq.processingService";
const string temporaryFileEnding = ".tmp";
const int defaultChunkSize = 900000 /* ~0.9 mb */;
const int partitionCount = 5;

var concurrentChunkStatusDictionary = new ConcurrentDictionary<string, ChunkStatus[]>();
var concurrentFileLockDictionary = new ConcurrentDictionary<string, SemaphoreSlim>();

Directory.CreateDirectory(targetFolder);

Console.WriteLine($"{appName} is running");
var cancellationTask = SetupCancellation(out var token);

try
{
    var consumerTasks = new Task[partitionCount];

    for (var consumerIndex = 0; consumerIndex < partitionCount; consumerIndex++)
    {
        var consumer = SetupConsumer();
        Console.WriteLine($"Consumer {consumer.Name} was initialized");

        consumerTasks[consumerIndex] = RunConsumerAsync(consumer, token);
    }

    await Task.WhenAll(consumerTasks);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Application was shutdown gracefully...");
}

async Task RunConsumerAsync(IConsumer<Null, byte[]> consumer, CancellationToken token)
{
    try
    {
        while (!token.IsCancellationRequested)
        {
            Console.WriteLine($"{consumer.Name} waiting for a message...");
            var consumeResult = await Task.Run(() => consumer.Consume(token), token);

            Console.WriteLine($"{consumer.Name} received a message");
            await ProcessFileAsync(consumeResult, token);

            consumer.Commit(consumeResult);
            Console.WriteLine($"{consumer.Name} consumed a message");
        }
    }
    finally
    {
        consumer.Close();
        consumer.Dispose();
    }
}

IConsumer<Null, byte[]> SetupConsumer()
{
    var kafkaConfig = new ConsumerConfig()
    {
        BootstrapServers = kafkaAddress,
        AllowAutoCreateTopics = true,
        GroupId = appName,
        EnableAutoCommit = false,
        IsolationLevel = IsolationLevel.ReadCommitted,
    };

    var consumer = new ConsumerBuilder<Null, byte[]>(kafkaConfig)
        .SetValueDeserializer(Deserializers.ByteArray).Build();

    consumer.Subscribe(topicName);

    return consumer;
}

async Task ProcessFileAsync(ConsumeResult<Null, byte[]> consumeResult, CancellationToken token)
{
    token.ThrowIfCancellationRequested();

    var messageMetadata = ExtractMessageMetadata(consumeResult.Message.Headers);

    var fileName = messageMetadata["FileName"];
    var position = int.Parse(messageMetadata["Position"]);
    var totalChunks = int.Parse(messageMetadata["TotalChunks"]);

    var filePath = Path.Combine(targetFolder, fileName);

    Console.WriteLine($"{fileName}'s {position} chunk was received.");
    await StoreFileDataAsync(filePath, position, totalChunks, consumeResult.Message.Value, token);
};

async Task StoreFileDataAsync(string filePath, int position, int totalChunks, byte[] data, CancellationToken token)
{
    var temporaryFileName = filePath + temporaryFileEnding;

    var totalLength = totalChunks * defaultChunkSize;

    var fileLock = GetReaderWriterLockForFile(filePath);

    await fileLock.WaitAsync();

    await using (var fs = File.OpenWrite(temporaryFileName))
    {
        if (fs.Length < totalLength)
        {
            fs.SetLength(totalLength);
        }

        fs.Position = position * defaultChunkSize;
        await fs.WriteAsync(data, token);
        Console.WriteLine($"#{position} chunk was written to {filePath}...");
    }

    fileLock.Release(1);

    UpdateFileStatus(filePath, position, totalChunks, out var isCompleted);

    if (isCompleted)
    {
        await TrimFileAsync(temporaryFileName, token);
        File.Move(temporaryFileName, filePath, true);
        Console.WriteLine($"{filePath} was downloaded");
    }
}

void UpdateFileStatus(string filePath, int position, int totalChunks, out bool isCompleted)
{
    isCompleted = false;

    if (!concurrentChunkStatusDictionary.TryGetValue(filePath, out var missedChunks))
    {
        missedChunks = new ChunkStatus[totalChunks];
        concurrentChunkStatusDictionary.TryAdd(filePath, missedChunks);
    }

    missedChunks[position] = ChunkStatus.Completed;

    if (!IsFileCompleted(filePath)) return;

    concurrentChunkStatusDictionary.TryRemove(filePath, out _);
    isCompleted = true;
}

bool IsFileCompleted(string filePath) =>
    !concurrentChunkStatusDictionary.ContainsKey(filePath) ||
    concurrentChunkStatusDictionary[filePath].All(c => c == ChunkStatus.Completed);

SemaphoreSlim GetReaderWriterLockForFile(string filePath) =>
concurrentFileLockDictionary.GetOrAdd(filePath, _ => new SemaphoreSlim(1, 1));

async Task TrimFileAsync(string filePath, CancellationToken token)
{
    var fileLock = GetReaderWriterLockForFile(filePath);

    await fileLock.WaitAsync();

    await using var fs = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
    fs.Position = fs.Length - defaultChunkSize;

    var buffer = new byte[defaultChunkSize];

    await fs.ReadAsync(buffer, token);

    var nullCharactersCount = buffer.Reverse().TakeWhile(b => b == default).Count();

    if (nullCharactersCount != 0) fs.SetLength(fs.Length - nullCharactersCount);

    fileLock.Release(1);
}

static IDictionary<string, string> ExtractMessageMetadata(Headers headers) =>
    headers.ToDictionary(h => h.Key, h => Encoding.Default.GetString(h.GetValueBytes()));

static Task SetupCancellation(out CancellationToken token)
{
    var cts = new CancellationTokenSource();

    token = cts.Token;

    return Task.Factory.StartNew(() =>
    {
        Console.WriteLine("Press ESC to shutdown...");
        while (!cts.IsCancellationRequested)
        {
            if (Console.ReadKey(true).Key == ConsoleKey.Escape) cts.Cancel();
        }
    }, TaskCreationOptions.LongRunning);
}