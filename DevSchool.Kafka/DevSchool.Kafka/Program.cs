using Confluent.Kafka;
using DevSchool.Kafka;
using Mindbox.Kafka;
using Mindbox.Kafka.Abstractions;
using Mindbox.Kafka.Template;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services.AddLogging();

builder.Services.AddSingleton<IAsyncConsumerHostFactory, AsyncConsumerHostFactory>();

builder.Services.AddKafkaProducerDependencies(
	builder.Configuration,
	provider => (_, exception, _) =>
	{
		var errorLogger = provider.GetRequiredService<ILogger<ProducerFactory>>();
		errorLogger.LogError(exception, "Error occured");
	});

builder.Services.AddSingleton<IAsyncConsumerMessageProcessor<string>, MyMessageProcessor>();
builder.Services.AddHostedService<KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerService>();

builder.Services.AddSingleton(new ConsumerConfig
{
	BootstrapServers = "localhost:29091",
	GroupId = "demo-consumer-group",
	AutoOffsetReset = AutoOffsetReset.Earliest,
	EnableAutoCommit = true
});

var app = builder.Build();
app.MapGet("/", () => "Kafka Producer and Consumer running...");
app.Run();