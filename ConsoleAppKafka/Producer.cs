using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace ConsoleAppKafka
{
    public class Producer
    {
        private readonly ConsumerConfig _config;

        public Producer(string server)
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = server,
            };
        }

        public void Send(string topic, string message)
        {
            Action<DeliveryReport<Null, string>> handler = r =>
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");

            using var p = new ProducerBuilder<Null, string>(_config).Build();
            p.Produce(topic, new Message<Null, string> { Value = message }, handler);

            // wait for up to 10 seconds for any inflight messages to be delivered.
            p.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
