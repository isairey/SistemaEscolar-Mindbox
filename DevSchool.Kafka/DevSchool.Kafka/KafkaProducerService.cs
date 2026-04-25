using Confluent.Kafka;
using Mindbox.Kafka;
using Mindbox.Kafka.Abstractions;

namespace DevSchool.Kafka;

public class KafkaProducerService : BackgroundService
{
	private readonly ILogger<KafkaProducerService> _logger;
	private readonly IProducer _producer;

	public KafkaProducerService(
		IProducerFactory producerFactory,
		IKafkaTopicsManagerFactory kafkaTopicsManagerFactory,
		ILogger<KafkaProducerService> logger)
	{
		_producer = Producers.Reliable(
			"producer-key",
			() => producerFactory,
			kafkaTopicsManagerFactory,
			GetTopicSpecificationParameters(),
			new ProducerConfigParameters
			{
				MessageTimeout = TimeSpan.FromSeconds(3),
				Linger = TimeSpan.FromMilliseconds(100),
				BatchSize = 2097176,
				MessageMaxBytes = 2097176,
				SocketNagleDisable = true,
				TopicMetadataPropagationMaxTimeout = TimeSpan.FromMilliseconds(700),
				CompressionType = CompressionType.Gzip
			},
			enableIdempotence: false,
			delayBeforeReproduce: TimeSpan.FromMilliseconds(1200));

		_logger = logger;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var random = new Random();
		var counter = 0;
		while (!stoppingToken.IsCancellationRequested)
		{
			var batchSize = GetBatchSize(random);

			var messages = Enumerable
				.Range(counter, batchSize)
				.Select(i => $"Message #{i} at {DateTime.UtcNow:O}");

			counter += batchSize;

			var options = new ParallelOptions
			{
				MaxDegreeOfParallelism = 20,
				CancellationToken = stoppingToken
			};

			await Parallel.ForEachAsync(
				messages,
				options,
				async (message, token) =>
				{
					_logger.LogInformation("Produced: {Message}", message);

					await _producer.ProduceAsync(
						"localhost:29091,localhost:29092,localhost:29093",
						"demo-topic",
						message,
						key: "some-key",
						token: token);
				});

			if (counter > 1000)
				throw new OperationCanceledException();
		}
	}

	private static int GetBatchSize(Random random)
	{
		var batchMessagesCount = random.Next(1, 6);

		if (batchMessagesCount > 4)
			batchMessagesCount = 100;

		return batchMessagesCount;
	}

	private static TopicSpecificationParameters GetTopicSpecificationParameters()
	{
		return new TopicSpecificationParameters(
			numPartitions: 3,
			replicationFactor: 3,
			configs: new Dictionary<string, string> { ["min.insync.replicas"] = "2" });
	}
}

// 10.2607720Z
// 26.1917490Z