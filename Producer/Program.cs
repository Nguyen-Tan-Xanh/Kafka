using Confluent.Kafka;
using System;
using System.Threading;

class Program
{
    public static void Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9091" // Update this to your Kafka server
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (true)
            {
                Console.Write("Enter message to send: ");
                string message = Console.ReadLine();

                try
                {
                    // Produce a message to the 'test-topic' topic
                    var result = producer.ProduceAsync("test1", new Message<Null, string> { Value = message }).Result;
                    Console.WriteLine($"Message sent to: {result.TopicPartitionOffset}");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Error sending message: {e.Error.Reason}");
                }
            }
        }
    }
}
