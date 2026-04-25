using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrapServers = "localhost:29091,localhost:29092,localhost:29093";
const string topicName = "new-topic";
const int numPartitions = 3;
const int replicationFactor = 3;

var config = new AdminClientConfig
{
	BootstrapServers = bootstrapServers
};

using var adminClient = new AdminClientBuilder(config).Build();

await adminClient.CreateTopicsAsync(
[
	new TopicSpecification
	{
		Name = topicName,
		NumPartitions = numPartitions,
		ReplicationFactor = replicationFactor,
		// Попробуй продьюсить в топик без min.insync.replicas
		// А потом в топик min.insync.replicas = 2 and 3
		// Во всех случаях отрубай 0, 1 или 2 брокера -- посмотри, что получится
		Configs = new Dictionary<string, string> { ["min.insync.replicas"] = "2" }
	}
]);

Console.WriteLine($"Топик '{topicName}' успешно создан.");

// Попробуй увеличить число партиций
/*await adminClient.CreatePartitionsAsync(
	[
		new PartitionsSpecification
		{
			Topic = topicName,
			IncreaseTo = 5
		}
	]);*/

// Попробуй удалить топик
//await adminClient.DeleteTopicsAsync([topicName]);

var producerConfig = new ProducerConfig
{
	BootstrapServers = bootstrapServers,
	MessageTimeoutMs = 2000,
	RetryBackoffMs = 300,
	Acks = Acks.All
};

using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

var result = await producer.ProduceAsync(topicName, new Message<string, string>
{
	// Попробуй продьюсить без ключа несколько раз -- убедись, что будут попадать сообщения в разные партиции
	// Попробуй с ключем MyKey2 -- убедись, что всегда в одну, потом с ключем MyKey -- убедись, что в другую
	// Key = "MyKey2",
	Value = "Hello, Kafka!"
});

Console.WriteLine($"Сообщение отправлено в {result.TopicPartitionOffset}");

// Попробуй прочитать сообщения из кафки
/*const string groupId = "my-consumer-group";

var config = new ConsumerConfig
{
	BootstrapServers = bootstrapServers,
	GroupId = groupId,
	// Убедись, что если ты топик еще не читал и подключишься на чтение с AutoOffsetReset.Latest,
	// то будешь ждать нового сообщения, а если с AutoOffsetReset.Earliest, то вычитаешь все существующие сначала
	AutoOffsetReset = AutoOffsetReset.Earliest,
	EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

consumer.Subscribe(topicName);

try
{
	while (true)
	{
		var cr = consumer.Consume();
		Console.WriteLine($"Получено сообщение: '{cr.Message.Value}' из {cr.TopicPartitionOffset}");
	}
}
catch (OperationCanceledException)
{
	consumer.Close();
}*/
























/*var producerConfig = new ProducerConfig
{
	BootstrapServers = bootstrapServers,
	MessageTimeoutMs = 2000,
	RetryBackoffMs = 300
};

using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

var result = await producer.ProduceAsync(topicName, new Message<Null, string>
{
//	Key = "MyKey2",
	Value = "Hello, Kafka!"
});

Console.WriteLine($"Сообщение отправлено в {result.TopicPartitionOffset}");*/

/*const string groupId = "my-consumer-group";

var config = new ConsumerConfig
{
	BootstrapServers = bootstrapServers,
	GroupId = groupId,
	AutoOffsetReset = AutoOffsetReset.Earliest, // читать с начала, если нет коммита
	EnableAutoCommit = true
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

consumer.Subscribe(topicName);

Console.WriteLine($"Ожидание сообщений из топика '{topicName}'...");

try
{
	while (true)
	{
		var cr = consumer.Consume();
		Console.WriteLine($"Получено сообщение: '{cr.Message.Value}' из {cr.TopicPartitionOffset}");
	}
}
catch (OperationCanceledException)
{
	// Корректно завершить
	consumer.Close();
}*/