using Confluent.Kafka;
using System;

class Program
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9091", // Update this to your Kafka server
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest // Start from the beginning if no previous offset exists
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("test1");

            Console.WriteLine("Consuming messages from 'test1'...");

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        Console.WriteLine($"Message received: {consumeResult.Message.Value}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation here (e.g. when pressing Ctrl+C)
                consumer.Close();
            }
        }
    }
}
