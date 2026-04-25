using Mindbox.Kafka;
using Mindbox.Kafka.Abstractions;

namespace DevSchool.Kafka;

public class KafkaConsumerService : BackgroundService
{
	private readonly IAsyncConsumerHostFactory _consumerHostFactory;
	private readonly IAsyncConsumerMessageProcessor<string> _messageProcessor;

	private readonly AsyncConsumerHostSettings _consumerHostSettings = new()
	{
		InternalChannelSize = 20,
		CommitLogSize = 1,
		WorkerTaskCount = 1,
		CommitterDelay = TimeSpan.FromMilliseconds(1000),
		SessionTimeoutMs = 9_000
	};

	public KafkaConsumerService(
		IAsyncConsumerHostFactory consumerHostFactory,
		IAsyncConsumerMessageProcessor<string> messageProcessor)
	{
		_consumerHostFactory = consumerHostFactory;
		_messageProcessor = messageProcessor;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var host = _consumerHostFactory.CreateHost(
			new Topic(
				() => "demo-topic",
				() => "localhost:29091,localhost:29092,localhost:29093",
				OffsetLossHandling.RedeliverHistoricalData),
			"demo-consumer-group",
			ConsumerFactories.CommonConsumerBuilderFactory(),
			_messageProcessor,
			_consumerHostSettings);

		await host.RunAsync(stoppingToken);
	}
}