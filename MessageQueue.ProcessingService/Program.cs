using System.Text;
using Confluent.Kafka;
using MessageQueue.ProcessingService;

const string appName = "MessageQueue.ProcessingService";
const string kafkaAddress = "host.docker.internal:9092";
const string targetFolder = "Destination";
const string topicName = "mq.processingService";
const string temporaryFileEnding = ".tmp";
const int defaultChunkSize = 900000 /* ~0.9 mb */;

var chunkStatusDictionary = new Dictionary<string, ChunkStatus[]>();

Directory.CreateDirectory(targetFolder);

var kafkaConfig = new ConsumerConfig()
{
    BootstrapServers = kafkaAddress,
    AllowAutoCreateTopics = true,
    GroupId = Guid.NewGuid().ToString(),
    EnableAutoCommit = false,
    ClientId = $"{appName}.{Guid.NewGuid():N}"
};

var consumer = new ConsumerBuilder<Null, byte[]>(kafkaConfig)
    .SetValueDeserializer(Deserializers.ByteArray).Build();

consumer.Subscribe(topicName);

try
{
    Console.WriteLine($"{appName} is running");

    var cancellationTask = SetupCancellation(out var token);

    ProcessFileAsync(token);

    await cancellationTask;
}
catch (OperationCanceledException)
{
    Console.WriteLine("Application was shutdown gracefully...");
}
finally
{
    consumer.Dispose();
}

async Task ProcessFileAsync(CancellationToken token) => await Task.Factory.StartNew(async () =>
{
    while (!token.IsCancellationRequested)
    {
        token.ThrowIfCancellationRequested();

        var consumeResult = consumer.Consume(token);
        var messageMetadata = ExtractMessageMetadata(consumeResult.Message.Headers);

        var fileName = messageMetadata["FileName"];
        var position = int.Parse(messageMetadata["Position"]);
        var totalChunks = int.Parse(messageMetadata["TotalChunks"]);

        var filePath = Path.Combine(targetFolder, fileName);

        Console.WriteLine($"{fileName}'s {position} chunk was received.");
        await StoreFileDataAsync(filePath, position, totalChunks, consumeResult.Message.Value, token);

        consumer.Commit(consumeResult);
    }

}, TaskCreationOptions.LongRunning).Unwrap();

async Task StoreFileDataAsync(string filePath, int position, int totalChunks, byte[] data, CancellationToken token)
{
    var temporaryFileName = filePath + temporaryFileEnding;

    var totalLength = totalChunks * defaultChunkSize;

    await using (var fs = File.OpenWrite(temporaryFileName))
    {
        if (fs.Length < totalLength)
        {
            fs.SetLength(totalLength);
        }

        fs.Position = (position - 1) * defaultChunkSize;
        await fs.WriteAsync(data, token);
        Console.WriteLine($"#{position} chunk was written to {filePath}...");
    }

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

    if (!chunkStatusDictionary.TryGetValue(filePath, out var missedChunks))
    {
        missedChunks = new ChunkStatus[totalChunks];
        chunkStatusDictionary.Add(filePath, missedChunks);
    }

    missedChunks[position - 1] = ChunkStatus.Completed;

    if (!IsFileCompleted(filePath)) return;

    chunkStatusDictionary.Remove(filePath);
    isCompleted = true;
}

bool IsFileCompleted(string filePath) => !chunkStatusDictionary.ContainsKey(filePath) || chunkStatusDictionary[filePath].All(c => c == ChunkStatus.Completed);

static async Task TrimFileAsync(string filePath, CancellationToken token)
{
    await using var fs = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
    fs.Position = fs.Length - defaultChunkSize;

    var buffer = new byte[defaultChunkSize];

    await fs.ReadAsync(buffer, token);

    var nullCharactersCount = buffer.Reverse().TakeWhile(b => b == default).Count();

    if (nullCharactersCount != 0) fs.SetLength(fs.Length - nullCharactersCount);
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
        while (!cts.Token.IsCancellationRequested)
        {
            if (Console.ReadKey(true).Key == ConsoleKey.Escape) cts.Cancel();
        }
    }, TaskCreationOptions.LongRunning);
}