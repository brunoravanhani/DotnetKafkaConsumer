using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ConsoleAppConsumer
{
    public class Consumer
    {
        private readonly ConsumerConfig _config;

        public Consumer(string server, string groupId)
        {
            _config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = server,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        public void Consume(string topic)
        {
            using var c = new ConsumerBuilder<Ignore, string>(_config).Build();
            c.Subscribe(topic);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }
}
